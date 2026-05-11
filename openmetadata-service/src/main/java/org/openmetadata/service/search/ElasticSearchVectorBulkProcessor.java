package org.openmetadata.service.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import es.co.elastic.clients.transport.rest5_client.low_level.Request;
import es.co.elastic.clients.transport.rest5_client.low_level.Response;
import es.co.elastic.clients.transport.rest5_client.low_level.ResponseException;
import es.co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.openmetadata.service.apps.bundles.searchIndex.stats.StageStatsTracker;
import org.openmetadata.service.apps.bundles.searchIndex.stats.StatsResult;

/**
 * Elasticsearch counterpart to {@link VectorBulkProcessor}. Speaks the bulk NDJSON protocol
 * directly via {@link Rest5Client} so we don't depend on the high-level ElasticsearchClient's
 * typed bulk API.
 */
@Slf4j
public class ElasticSearchVectorBulkProcessor implements AutoCloseable {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int DEFAULT_MAX_BULK_ACTIONS = 500;
  private static final long DEFAULT_MAX_PAYLOAD_BYTES = 50L * 1024 * 1024;
  private static final long FLUSH_INTERVAL_MS = 1000;

  private final Rest5Client restClient;
  private final String targetIndex;
  private final int maxBulkActions;
  private final long maxPayloadBytes;
  private final ScheduledExecutorService scheduler;

  private final List<VectorChunk> buffer = new ArrayList<>();
  private final AtomicLong payloadSize = new AtomicLong(0);
  private final AtomicLong totalSuccess = new AtomicLong(0);
  private final AtomicLong totalFailed = new AtomicLong(0);
  private volatile boolean closed = false;
  private StageStatsTracker statsTracker;

  public record VectorChunk(String chunkId, Map<String, Object> document, long estimatedSize) {}

  public ElasticSearchVectorBulkProcessor(Rest5Client restClient, String targetIndex) {
    this(restClient, targetIndex, DEFAULT_MAX_BULK_ACTIONS, DEFAULT_MAX_PAYLOAD_BYTES);
  }

  public ElasticSearchVectorBulkProcessor(
      Rest5Client restClient, String targetIndex, int maxBulkActions, long maxPayloadBytes) {
    this.restClient = restClient;
    this.targetIndex = targetIndex;
    this.maxBulkActions = maxBulkActions;
    this.maxPayloadBytes = maxPayloadBytes;
    this.scheduler = Executors.newScheduledThreadPool(1);
    scheduler.scheduleAtFixedRate(
        this::flushIfNeeded, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
  }

  public String getTargetIndex() {
    return targetIndex;
  }

  public void setStatsTracker(StageStatsTracker tracker) {
    this.statsTracker = tracker;
  }

  public synchronized void addChunk(String chunkId, Map<String, Object> chunkDoc) {
    long estimated = estimateChunkSize(chunkDoc);
    if (shouldFlush(estimated)) {
      flush();
    }
    buffer.add(new VectorChunk(chunkId, chunkDoc, estimated));
    payloadSize.addAndGet(estimated);
  }

  public synchronized void flush() {
    if (buffer.isEmpty()) {
      return;
    }

    List<VectorChunk> toFlush = new ArrayList<>(buffer);
    buffer.clear();
    payloadSize.set(0);

    try {
      // Build NDJSON: action header line + doc line, repeated.
      StringBuilder body = new StringBuilder();
      for (VectorChunk chunk : toFlush) {
        body.append("{\"index\":{\"_index\":\"")
            .append(targetIndex)
            .append("\",\"_id\":\"")
            .append(chunk.chunkId().replace("\"", "\\\""))
            .append("\"}}\n");
        body.append(MAPPER.writeValueAsString(chunk.document())).append('\n');
      }

      Request request = new Request("POST", "/_bulk");
      request.setJsonEntity(body.toString());

      int success = 0;
      int failed = 0;
      try {
        Response response = restClient.performRequest(request);
        var responseJson = parseBulkResponse(response);
        success = responseJson[0];
        failed = responseJson[1];
      } catch (ResponseException e) {
        // The whole bulk request was rejected (e.g. 4xx/5xx on the endpoint itself).
        failed = toFlush.size();
        LOG.error(
            "ES vector bulk flush rejected for index {}: status {}",
            targetIndex,
            e.getResponse().getStatusCode());
      }

      totalSuccess.addAndGet(success);
      totalFailed.addAndGet(failed);

      if (statsTracker != null) {
        for (int i = 0; i < success; i++) {
          statsTracker.recordVector(StatsResult.SUCCESS);
        }
        for (int i = 0; i < failed; i++) {
          statsTracker.recordVector(StatsResult.FAILED);
        }
      }

      if (failed > 0) {
        LOG.warn(
            "ES vector bulk flush: {} success, {} failed out of {} in {}",
            success,
            failed,
            toFlush.size(),
            targetIndex);
      } else {
        LOG.debug("ES vector bulk flush: {} documents indexed in {}", success, targetIndex);
      }
    } catch (Exception e) {
      totalFailed.addAndGet(toFlush.size());
      if (statsTracker != null) {
        for (int i = 0; i < toFlush.size(); i++) {
          statsTracker.recordVector(StatsResult.FAILED);
        }
      }
      LOG.error(
          "ES vector bulk flush failed for {} documents in {}: {}",
          toFlush.size(),
          targetIndex,
          e.getMessage(),
          e);
    }
  }

  /** Returns {success, failed}. Parses the {@code items[]} of an ES bulk response. */
  private int[] parseBulkResponse(Response response) throws java.io.IOException {
    int success = 0;
    int failed = 0;
    if (response.getEntity() != null) {
      try (InputStream is = response.getEntity().getContent()) {
        String body = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        var root = MAPPER.readTree(body);
        var items = root.path("items");
        if (items.isArray()) {
          for (var item : items) {
            // Each item is e.g. {"index":{"_id":..., "status":201}} — error key present on
            // failure.
            var indexNode = item.path("index");
            if (!indexNode.path("error").isMissingNode()) {
              failed++;
            } else {
              success++;
            }
          }
        }
      }
    }
    return new int[] {success, failed};
  }

  private synchronized void flushIfNeeded() {
    if (!buffer.isEmpty() && !closed) {
      flush();
    }
  }

  private boolean shouldFlush(long additionalSize) {
    return buffer.size() >= maxBulkActions || payloadSize.get() + additionalSize > maxPayloadBytes;
  }

  private long estimateChunkSize(Map<String, Object> doc) {
    long size = 0;
    Object embedding = doc.get("embedding");
    if (embedding instanceof float[] arr) {
      size += (long) arr.length * 4;
    }
    for (Map.Entry<String, Object> entry : doc.entrySet()) {
      if ("embedding".equals(entry.getKey())) continue;
      Object value = entry.getValue();
      if (value instanceof String s) {
        size += s.length() * 2L;
      } else if (value instanceof List<?> list) {
        size += list.size() * 50L;
      }
    }
    return (long) (size * 1.2);
  }

  public long getTotalSuccess() {
    return totalSuccess.get();
  }

  public long getTotalFailed() {
    return totalFailed.get();
  }

  @Override
  public void close() {
    closed = true;
    scheduler.shutdown();
    flush();
  }
}
