package com.vicarius.vicarius.service;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Service
public class ElasticService {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    Logger logger = LoggerFactory.getLogger(ElasticService.class);

    public ResponseEntity<Map<String, Object>> createIndex(String indexName) {

        CreateIndexRequest request = new CreateIndexRequest(indexName);
        try {
            restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        }
        catch (ElasticsearchStatusException e) {
            return handleElasticSearchStatusException(e);
        } catch (IOException e) {
            return handleIOException(e);
        }
        return null;
    }

    public ResponseEntity<Map<String, Object>> createDocument(String indexName, Map<String, Object> document) {

        // Check if index doesn't exist
        GetIndexRequest request = new GetIndexRequest(indexName);
        try {
            if (!restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT)) {
                Map<String, Object> resMap = new HashMap<>();
                resMap.put("Message", "Index does not exists, First create the index.");
                return new ResponseEntity<>(resMap,HttpStatus.NOT_FOUND);
            }
        } catch (IOException e) {
            return handleIOException(e);
        }

        IndexRequest indexRequest = new IndexRequest(indexName);
        indexRequest.source(document);

        try {
            IndexResponse res = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
            Map<String, Object> resMap = new HashMap<>();
            resMap.put("document_id", res.getId());
            return new ResponseEntity<>(resMap, HttpStatus.CREATED);
        } catch (IOException e) { // Handle error
            return handleIOException(e);
        }
    }

    public ResponseEntity<Map<String, Object>> getDocumentById(String indexName, String id) {

        GetRequest getRequest = new GetRequest(indexName, id);
        try {
            GetResponse res = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);

            return res.isExists() ? new ResponseEntity<>(res.getSourceAsMap(),HttpStatus.OK) : new ResponseEntity<>(HttpStatus.NOT_FOUND);
        } catch (ElasticsearchStatusException e) {
            return handleElasticSearchStatusException(e);
        } catch (IOException e) {
            return handleIOException(e);
        }
    }

    private ResponseEntity<Map<String, Object>> handleIOException(IOException e) {
        logger.error(e.getMessage());
        Map<String, Object> resMap = new HashMap<>();
        resMap.put("message", "Something went wrong.");
        return new ResponseEntity<>(resMap, HttpStatus.INTERNAL_SERVER_ERROR);
    }

    private ResponseEntity<Map<String, Object>> handleElasticSearchStatusException(ElasticsearchStatusException e) {
        logger.error(e.getMessage());
        Map<String, Object> resMap = new HashMap<>();
        resMap.put("message", e.getMessage().substring(e.getMessage().indexOf("reason")));
        return new ResponseEntity<>(resMap,HttpStatus.valueOf(e.status().getStatus()));
    }
}
