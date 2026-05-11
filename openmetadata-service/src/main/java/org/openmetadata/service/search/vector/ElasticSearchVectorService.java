package org.openmetadata.service.search.vector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import es.co.elastic.clients.elasticsearch.ElasticsearchClient;
import es.co.elastic.clients.transport.rest5_client.Rest5ClientTransport;
import es.co.elastic.clients.transport.rest5_client.low_level.Request;
import es.co.elastic.clients.transport.rest5_client.low_level.Response;
import es.co.elastic.clients.transport.rest5_client.low_level.ResponseException;
import es.co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.openmetadata.schema.EntityInterface;
import org.openmetadata.service.events.lifecycle.EntityLifecycleEventDispatcher;
import org.openmetadata.service.search.ElasticSearchVectorBulkProcessor;
import org.openmetadata.service.search.vector.client.EmbeddingClient;
import org.openmetadata.service.search.vector.utils.DTOs.VectorSearchResponse;

/**
 * Elasticsearch 8.x/9.x vector search service. Mirrors {@link OpenSearchVectorService} but uses
 * the Elasticsearch {@code dense_vector} field type and the top-level {@code knn} query format,
 * and speaks to the cluster via the low-level {@link Rest5Client} so we don't depend on the
 * typed ES Java API for ad-hoc requests.
 */
@Slf4j
public class ElasticSearchVectorService implements VectorIndexService {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int OVER_FETCH_MULTIPLIER = 2;

  private static volatile ElasticSearchVectorService instance;

  private final ElasticsearchClient client;
  private final Rest5Client restClient;
  @Getter private final EmbeddingClient embeddingClient;
  private final String language;
  private final int knnNumCandidatesMultiplier;
  private ElasticSearchVectorBulkProcessor centralBulkProcessor;
  private String centralBulkProcessorIndex;

  public ElasticSearchVectorService(
      ElasticsearchClient client,
      EmbeddingClient embeddingClient,
      String language,
      int knnNumCandidatesMultiplier) {
    this.client = client;
    this.restClient = extractRestClient(client);
    this.embeddingClient = embeddingClient;
    this.language = language != null ? language.toLowerCase(java.util.Locale.ROOT) : "en";
    this.knnNumCandidatesMultiplier =
        knnNumCandidatesMultiplier > 0
            ? knnNumCandidatesMultiplier
            : VectorSearchQueryBuilder.DEFAULT_KNN_NUM_CANDIDATES_MULTIPLIER;
  }

  public ElasticSearchVectorService(
      ElasticsearchClient client, EmbeddingClient embeddingClient, String language) {
    this(
        client,
        embeddingClient,
        language,
        VectorSearchQueryBuilder.DEFAULT_KNN_NUM_CANDIDATES_MULTIPLIER);
  }

  public ElasticSearchVectorService(ElasticsearchClient client, EmbeddingClient embeddingClient) {
    this(client, embeddingClient, "en");
  }

  private static Rest5Client extractRestClient(ElasticsearchClient client) {
    if (!(client._transport() instanceof Rest5ClientTransport rest5)) {
      throw new IllegalArgumentException(
          "ElasticSearchVectorService requires Rest5ClientTransport, got: "
              + client._transport().getClass().getName());
    }
    return rest5.restClient();
  }

  public static synchronized void init(
      ElasticsearchClient client, EmbeddingClient embeddingClient, String language) {
    init(
        client,
        embeddingClient,
        language,
        VectorSearchQueryBuilder.DEFAULT_KNN_NUM_CANDIDATES_MULTIPLIER);
  }

  public static synchronized void init(
      ElasticsearchClient client,
      EmbeddingClient embeddingClient,
      String language,
      int knnNumCandidatesMultiplier) {
    if (instance != null) {
      LOG.warn("ElasticSearchVectorService already initialized, reinitializing");
    }
    instance =
        new ElasticSearchVectorService(
            client, embeddingClient, language, knnNumCandidatesMultiplier);
    instance.registerVectorEmbeddingHandler();
    LOG.info(
        "ElasticSearchVectorService initialized with model={}, dimension={}",
        embeddingClient.getModelId(),
        embeddingClient.getDimension());
  }

  public static ElasticSearchVectorService getInstance() {
    return instance;
  }

  private void registerVectorEmbeddingHandler() {
    try {
      VectorEmbeddingHandler handler = new VectorEmbeddingHandler(this);
      EntityLifecycleEventDispatcher.getInstance().registerHandler(handler);
      LOG.info("Registered VectorEmbeddingHandler for entity lifecycle events");
    } catch (Exception e) {
      LOG.error("Failed to register VectorEmbeddingHandler", e);
    }
  }

  public void close() {
    // No-op by design — see OpenSearchVectorService.close() for context. The transport is
    // shared with the rest of the application; closing it here would shut down the whole
    // HC5 IOReactor.
  }

  @Override
  @SuppressWarnings("unchecked")
  public VectorSearchResponse search(
      String query, Map<String, List<String>> filters, int size, int k, double threshold) {
    long start = System.currentTimeMillis();
    try {
      float[] queryVector = embeddingClient.embed(query);
      int overFetchSize = size * OVER_FETCH_MULTIPLIER;

      String queryJson =
          VectorSearchQueryBuilder.buildNativeESQuery(
              queryVector, overFetchSize, k, filters, knnNumCandidatesMultiplier);
      String indexName = getClusteredIndexName();
      String responseBody = executeGenericRequest("POST", "/" + indexName + "/_search", queryJson);

      JsonNode root = MAPPER.readTree(responseBody);
      JsonNode hitsNode = root.path("hits").path("hits");

      LinkedHashMap<String, List<Map<String, Object>>> byParent = new LinkedHashMap<>();
      for (JsonNode hit : hitsNode) {
        double score = hit.path("_score").asDouble(0.0);
        if (score < threshold) {
          continue;
        }

        Map<String, Object> hitMap = MAPPER.convertValue(hit.path("_source"), Map.class);
        hitMap.put("_score", score);

        String parentId = (String) hitMap.get("parent_id");
        if (parentId == null) {
          parentId = hit.path("_id").asText();
          hitMap.put("parent_id", parentId);
        }
        byParent.computeIfAbsent(parentId, kVal -> new ArrayList<>()).add(hitMap);
      }

      List<Map<String, Object>> results = new ArrayList<>();
      int parentCount = 0;
      for (List<Map<String, Object>> chunks : byParent.values()) {
        if (parentCount >= size) {
          break;
        }
        results.addAll(chunks);
        parentCount++;
      }

      long tookMillis = System.currentTimeMillis() - start;
      return new VectorSearchResponse(tookMillis, results);
    } catch (Exception e) {
      LOG.error("Vector search failed: {}", e.getMessage(), e);
      throw new RuntimeException("Vector search failed", e);
    }
  }

  String executeGenericRequest(String method, String endpoint, String body) {
    try {
      Request request = new Request(method, endpoint);
      if (body != null) {
        request.setJsonEntity(body);
      }
      // Rest5Client.performRequest only throws ResponseException on 5xx (its internal
      // isCorrectServerResponse is `code < 500`). 4xx responses are returned normally,
      // so we still need a manual status-code check below for client errors.
      Response response = restClient.performRequest(request);
      int statusCode = response.getStatusCode();
      String responseBody = readEntityBody(response.getEntity());
      if (statusCode >= 400) {
        throw new RuntimeException(
            "Elasticsearch request failed with status " + statusCode + ": " + responseBody);
      }
      return responseBody;
    } catch (ResponseException e) {
      int statusCode = e.getResponse().getStatusCode();
      String errorBody = readEntityBody(e.getResponse().getEntity());
      LOG.error("Generic request failed: {} {}", method, endpoint, e);
      throw new RuntimeException(
          "Elasticsearch request failed with status " + statusCode + ": " + errorBody, e);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      LOG.error("Generic request failed: {} {}", method, endpoint, e);
      throw new RuntimeException("Elasticsearch generic request failed", e);
    }
  }

  private static String readEntityBody(org.apache.hc.core5.http.HttpEntity entity) {
    if (entity == null) {
      return "";
    }
    try (InputStream is = entity.getContent()) {
      return new String(is.readAllBytes(), StandardCharsets.UTF_8);
    } catch (Exception ignored) {
      return "";
    }
  }

  @Override
  public void updateVectorEmbeddings(EntityInterface entity, String targetIndex) {
    try {
      String parentId = entity.getId().toString();
      String existingFingerprint = getExistingFingerprint(targetIndex, parentId);
      String currentFingerprint = VectorDocBuilder.computeFingerprintForEntity(entity);

      if (currentFingerprint.equals(existingFingerprint)) {
        LOG.debug("Skipping entity {} - fingerprint unchanged", parentId);
        return;
      }

      List<Map<String, Object>> docs = VectorDocBuilder.fromEntity(entity, embeddingClient);
      deleteByParentId(targetIndex, parentId);
      bulkIndex(docs, targetIndex);
    } catch (Exception e) {
      LOG.error(
          "Failed to update vector embeddings for entity {}: {}",
          entity.getId(),
          e.getMessage(),
          e);
    }
  }

  @Override
  public void updateVectorEmbeddingsWithMigration(
      EntityInterface entity, String targetIndex, String sourceIndex) {
    try {
      String parentId = entity.getId().toString();
      String currentFingerprint = VectorDocBuilder.computeFingerprintForEntity(entity);

      if (sourceIndex != null) {
        try {
          String existingFingerprint = getExistingFingerprint(sourceIndex, parentId);
          if (currentFingerprint.equals(existingFingerprint)) {
            if (copyExistingVectorDocuments(
                sourceIndex, targetIndex, parentId, currentFingerprint)) {
              return;
            }
          }
        } catch (Exception ex) {
          LOG.warn(
              "Migration copy failed for entity {}, falling back to recomputation: {}",
              parentId,
              ex.getMessage());
        }
      }

      List<Map<String, Object>> docs = VectorDocBuilder.fromEntity(entity, embeddingClient);
      bulkIndex(docs, targetIndex);
    } catch (Exception e) {
      LOG.error(
          "Failed to update vector embeddings with migration for entity {}: {}",
          entity.getId(),
          e.getMessage(),
          e);
    }
  }

  @Override
  public String getExistingFingerprint(String indexName, String parentId) {
    try {
      String query =
          "{\"size\":1,\"_source\":[\"fingerprint\"],"
              + "\"query\":{\"term\":{\"parent_id\":\""
              + VectorSearchQueryBuilder.escape(parentId)
              + "\"}}}";
      String response = executeGenericRequest("POST", "/" + indexName + "/_search", query);
      JsonNode root = MAPPER.readTree(response);
      JsonNode hits = root.path("hits").path("hits");
      if (hits.isArray() && !hits.isEmpty()) {
        return hits.get(0).path("_source").path("fingerprint").asText(null);
      }
    } catch (Exception e) {
      LOG.debug(
          "Failed to get fingerprint for parent_id={} in index={}: {}",
          parentId,
          indexName,
          e.getMessage());
    }
    return null;
  }

  @Override
  public Map<String, String> getExistingFingerprintsBatch(
      String indexName, List<String> parentIds) {
    if (parentIds == null || parentIds.isEmpty()) {
      return Collections.emptyMap();
    }
    try {
      StringBuilder termsArray = new StringBuilder("[");
      for (int i = 0; i < parentIds.size(); i++) {
        if (i > 0) termsArray.append(',');
        termsArray
            .append("\"")
            .append(VectorSearchQueryBuilder.escape(parentIds.get(i)))
            .append("\"");
      }
      termsArray.append("]");

      String query =
          "{\"size\":"
              + parentIds.size()
              + ",\"_source\":[\"parent_id\",\"fingerprint\"]"
              + ",\"query\":{\"terms\":{\"parent_id\":"
              + termsArray
              + "}}"
              + ",\"collapse\":{\"field\":\"parent_id\"}}";

      String response = executeGenericRequest("POST", "/" + indexName + "/_search", query);
      JsonNode root = MAPPER.readTree(response);
      JsonNode hits = root.path("hits").path("hits");

      Map<String, String> result = new HashMap<>();
      for (JsonNode hit : hits) {
        String pid = hit.path("_source").path("parent_id").asText();
        String fp = hit.path("_source").path("fingerprint").asText(null);
        if (pid != null && fp != null) {
          result.put(pid, fp);
        }
      }
      return result;
    } catch (Exception e) {
      LOG.error("Failed to batch get fingerprints in index={}: {}", indexName, e.getMessage(), e);
      return Collections.emptyMap();
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean copyExistingVectorDocuments(
      String sourceIndex, String targetIndex, String parentId, String fingerprint) {
    try {
      String searchQuery =
          "{\"size\":1000,\"query\":{\"term\":{\"parent_id\":\""
              + VectorSearchQueryBuilder.escape(parentId)
              + "\"}}}";
      String response = executeGenericRequest("POST", "/" + sourceIndex + "/_search", searchQuery);
      JsonNode root = MAPPER.readTree(response);
      JsonNode hits = root.path("hits").path("hits");

      if (!hits.isArray() || hits.isEmpty()) {
        return false;
      }

      List<Map<String, Object>> docs = new ArrayList<>();
      for (JsonNode hit : hits) {
        Map<String, Object> source = MAPPER.convertValue(hit.path("_source"), Map.class);
        source.put("fingerprint", fingerprint);
        docs.add(source);
      }
      bulkIndex(docs, targetIndex);
      return true;
    } catch (Exception e) {
      LOG.error(
          "Failed to copy vector documents from {} to {} for parent_id={}: {}",
          sourceIndex,
          targetIndex,
          parentId,
          e.getMessage(),
          e);
      return false;
    }
  }

  @Override
  public void softDeleteEmbeddings(EntityInterface entity) {
    try {
      String parentId = entity.getId().toString();
      String indexName = getClusteredIndexName();
      String script =
          "{\"script\":{\"source\":\"ctx._source.deleted = true\"},"
              + "\"query\":{\"term\":{\"parent_id\":\""
              + VectorSearchQueryBuilder.escape(parentId)
              + "\"}}}";
      executeGenericRequest("POST", "/" + indexName + "/_update_by_query", script);
    } catch (Exception e) {
      LOG.error(
          "Failed to soft delete embeddings for entity {}: {}", entity.getId(), e.getMessage(), e);
    }
  }

  @Override
  public void hardDeleteEmbeddings(EntityInterface entity) {
    try {
      String parentId = entity.getId().toString();
      String indexName = getClusteredIndexName();
      deleteByParentId(indexName, parentId);
    } catch (Exception e) {
      LOG.error(
          "Failed to hard delete embeddings for entity {}: {}", entity.getId(), e.getMessage(), e);
    }
  }

  @Override
  public void restoreEmbeddings(EntityInterface entity) {
    try {
      String parentId = entity.getId().toString();
      String indexName = getClusteredIndexName();
      String script =
          "{\"script\":{\"source\":\"ctx._source.deleted = false\"},"
              + "\"query\":{\"term\":{\"parent_id\":\""
              + VectorSearchQueryBuilder.escape(parentId)
              + "\"}}}";
      executeGenericRequest("POST", "/" + indexName + "/_update_by_query", script);
    } catch (Exception e) {
      LOG.error(
          "Failed to restore embeddings for entity {}: {}", entity.getId(), e.getMessage(), e);
    }
  }

  private void deleteByParentId(String indexName, String parentId) {
    try {
      String query =
          "{\"query\":{\"term\":{\"parent_id\":\""
              + VectorSearchQueryBuilder.escape(parentId)
              + "\"}}}";
      executeGenericRequest("POST", "/" + indexName + "/_delete_by_query", query);
    } catch (Exception e) {
      LOG.error(
          "Failed to delete by parent_id={} in index={}: {}",
          parentId,
          indexName,
          e.getMessage(),
          e);
    }
  }

  private static String getClusteredIndexName() {
    return VectorIndexService.getClusteredIndexName();
  }

  @Override
  public void createOrUpdateIndex(int dimension) {
    try {
      if (indexExists()) {
        LOG.info("Vector index {} already exists", VECTOR_INDEX_NAME);
        return;
      }

      String mappingJson = loadIndexMapping(dimension);
      executeGenericRequest("PUT", "/" + getClusteredIndexName(), mappingJson);
      LOG.info("Created vector index {} with dimension {}", getClusteredIndexName(), dimension);
    } catch (Exception e) {
      LOG.error("Failed to create vector index: {}", e.getMessage(), e);
    }
  }

  @Override
  public boolean indexExists() {
    try {
      Request request = new Request("HEAD", "/" + getClusteredIndexName());
      Response response = restClient.performRequest(request);
      return response.getStatusCode() == 200;
    } catch (ResponseException e) {
      return false;
    } catch (Exception e) {
      LOG.error("Failed to check if vector index exists: {}", e.getMessage(), e);
      return false;
    }
  }

  @Override
  public String getIndexName() {
    return getClusteredIndexName();
  }

  @Override
  public void bulkIndex(List<Map<String, Object>> documents, String targetIndex) {
    if (documents == null || documents.isEmpty()) {
      return;
    }

    ElasticSearchVectorBulkProcessor processor = getOrCreateBulkProcessor(targetIndex);
    for (int i = 0; i < documents.size(); i++) {
      Map<String, Object> doc = documents.get(i);
      String parentId = (String) doc.get("parent_id");
      int chunkIndex = doc.containsKey("chunk_index") ? (int) doc.get("chunk_index") : i;
      String docId = parentId + "-" + chunkIndex;
      processor.addChunk(docId, doc);
    }
  }

  private synchronized ElasticSearchVectorBulkProcessor getOrCreateBulkProcessor(
      String targetIndex) {
    if (centralBulkProcessor == null || !targetIndex.equals(centralBulkProcessorIndex)) {
      if (centralBulkProcessor != null) {
        centralBulkProcessor.close();
      }
      centralBulkProcessor = new ElasticSearchVectorBulkProcessor(restClient, targetIndex);
      centralBulkProcessorIndex = targetIndex;
    }
    return centralBulkProcessor;
  }

  public synchronized void flushBulkProcessor() {
    if (centralBulkProcessor != null) {
      centralBulkProcessor.close();
      centralBulkProcessor = null;
      centralBulkProcessorIndex = null;
    }
  }

  private String loadIndexMapping(int dimension) {
    String resourcePath = "elasticsearch/" + language + "/vector_search_index_es_native.json";
    try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
      if (inputStream == null) {
        throw new IllegalStateException("Could not find " + resourcePath + " in classpath");
      }
      String template = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
      // Templates ship with dims:512 as the placeholder; rewrite to the active dimension.
      String result = template.replace("\"dims\": 512", "\"dims\": " + dimension);
      if (result.equals(template) && dimension != 512) {
        throw new IllegalStateException(
            "Failed to replace dims placeholder in ES vector index mapping template");
      }
      return result;
    } catch (IOException e) {
      throw new RuntimeException("Failed to load ES vector search index mapping", e);
    }
  }
}
