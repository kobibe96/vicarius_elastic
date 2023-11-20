package com.vicarius.vicarius.controller;

import com.vicarius.vicarius.service.ElasticService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.Map;

@RestController
public class ElasticController {

    @Autowired
    private ElasticService elasticService;

    @PostMapping("/index/{indexName}")
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<Map<String, Object>> createIndex(@PathVariable String indexName) {
        return elasticService.createIndex(indexName);
    }

    @PostMapping("/document/{indexName}")
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<Map<String, Object>> createDocument(@PathVariable String indexName, @RequestBody Map<String, Object> document) {
        return elasticService.createDocument(indexName, document);
    }

    @GetMapping("/document/{indexName}/{id}")
    public ResponseEntity<Map<String, Object>> getDocumentById(@PathVariable String indexName, @PathVariable String id) {
        return elasticService.getDocumentById(indexName, id);
    }
}
