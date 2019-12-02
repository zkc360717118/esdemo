package com.example.esdemo.service;

import com.example.esdemo.document.Flight;
import com.example.esdemo.document.ProfileDocument;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import util.Constant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static util.Constant.INDEX;
import static util.Constant.TYPE;

@Service
@Slf4j
public class EsService {
    @Autowired
    private RestHighLevelClient client;

    @Autowired
    private ObjectMapper objectMapper;

    public String createProfileDocument(ProfileDocument document) throws IOException {
        UUID uuid = UUID.randomUUID(); //利用UUID自动生成一个随机id
        document.setId(uuid.toString());
        IndexRequest indexRequest = new IndexRequest(INDEX, TYPE, document.getId())
                .source(convertProfileDocumentToMap(document));
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);//开始索引一条document
        return indexResponse.getResult().name();
    }

    public ProfileDocument findById(String id) throws IOException {
        GetRequest getRequest = new GetRequest(INDEX, TYPE, id);
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        Map<String, Object> resultMap = getResponse.getSource();
        //把返回的map结果转化一下
        return convertMapToProfileDocument(resultMap);
    }

    public String updateProfie(ProfileDocument document) throws IOException {
        ProfileDocument resultDocument = findById(document.getId());
        UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, resultDocument.getId());
        updateRequest.doc(convertProfileDocumentToMap(document)); //设置需要修改的内容
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        return updateResponse.getResult().name();
    }

//    public List<ProfileDocument> findAll() throws IOException {
    public List<Flight> findAll() throws IOException {
        SearchRequest searchRequest = buildSearchRequest(INDEX, TYPE);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return getSearchResult(searchResponse);
    }

    public List<Flight> findProfileByName(String name) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(INDEX);
        searchRequest.types(TYPE);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name", name).operator(Operator.AND);
        searchSourceBuilder.query(matchQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return getSearchResult(searchResponse);
    }

    public String deleteProfileDocument(String id) throws Exception {
        DeleteRequest deleteRequest = new DeleteRequest(INDEX, TYPE, id);
        DeleteResponse resp = client.delete(deleteRequest, RequestOptions.DEFAULT);
        return resp.getResult().name();
    }

//    private List<ProfileDocument> getSearchResult(SearchResponse response) {
//    private List<ProfileDocument> getSearchResult(SearchResponse response) {
//        SearchHit[] hits = response.getHits().getHits();
//        ArrayList<ProfileDocument> profileDocuments = new ArrayList<>();
//        for (SearchHit hit : hits) {
//            profileDocuments
//                    .add(objectMapper.convertValue(hit.getSourceAsMap(), ProfileDocument.class));
//        }
//
//        return profileDocuments;
//    }

    private List<Flight> getSearchResult(SearchResponse response) {
        SearchHit[] hits = response.getHits().getHits();
        ArrayList<Flight> profileDocuments = new ArrayList<>();
        for (SearchHit hit : hits) {
            profileDocuments
                    .add(objectMapper.convertValue(hit.getSourceAsMap(), Flight.class));
        }

        return profileDocuments;
    }


    private Map<String, Object> convertProfileDocumentToMap(ProfileDocument document) {
        return objectMapper.convertValue(document, Map.class);
    }

    private ProfileDocument convertMapToProfileDocument(Map<String, Object> map) {
        return objectMapper.convertValue(map, ProfileDocument.class);
    }

    private SearchRequest buildSearchRequest(String index, String type) {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(index);
        searchRequest.types(type);
        return searchRequest;
    }
}
