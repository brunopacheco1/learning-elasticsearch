package com.dev.bruno.learning.elasticsearch;

import com.google.gson.Gson;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
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
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SearchTest {

    private static RestHighLevelClient highLevelClient;
    private static RestClient lowLevelClient;

    @BeforeClass
    public static void setUp() {
        highLevelClient = new RestHighLevelClient(
            RestClient.builder(
                new HttpHost("localhost", 9200, "http")
            )
        );

        lowLevelClient = highLevelClient.getLowLevelClient();
    }

    @AfterClass
    public static void close() throws IOException {
        highLevelClient.close();
    }

    @Test
    public void test1CreateIndex() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("bank");
        request.settings(Settings.builder()
            .put("index.number_of_shards", 5)
            .put("index.number_of_replicas", 0)
        );

        CreateIndexResponse createIndexResponse = highLevelClient.indices().create(request);

        assert createIndexResponse.isShardsAcknowledged();
        assert createIndexResponse.isAcknowledged();
    }

    @Test
    public void test2CreateDocuments() throws IOException, InterruptedException {
        InputStream in = new URL( "https://raw.githubusercontent.com/elastic/elasticsearch/master/docs/src/test/resources/accounts.json" ).openStream();

        String lowLevelRequestBody = readString(in) + "\n";

        HttpEntity entity = new NStringEntity(lowLevelRequestBody, ContentType.APPLICATION_JSON);
        Map<String, String> params = Collections.singletonMap("pretty", "true");

        Response response = lowLevelClient.performRequest("POST", "/bank/_doc/_bulk", params, entity);
        Map<String, Object> responseBody = readBody(response.getEntity().getContent());

        assert response.getStatusLine().getStatusCode() == 200;
        assert !((Boolean) responseBody.get("errors"));

        //Bulk is a async request
        Thread.sleep(5000L);
    }

    @Test
    public void test3FirstSearch() throws IOException {
        Response response = lowLevelClient.performRequest("GET", "/bank/_search?q=*&sort=account_number:asc&pretty");
        Map<String, Object> responseBody = readBody(response.getEntity().getContent());

        assert response.getStatusLine().getStatusCode() == 200;

        Map<String, Object> hits = (Map<String, Object>) responseBody.get("hits");
        Double total = (Double) hits.get("total");
        assert total == 1000;
    }

    @Test
    public void test4QueryLanguage() throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("age", "40"))
                .mustNot(QueryBuilders.matchQuery("state", "ID"))
        );

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = highLevelClient.search(searchRequest);

        assert searchResponse.getHits().getTotalHits() == 43;

        String lowLevelRequestBody = "{\n" +
            "  \"query\": {\n" +
            "    \"bool\": {\n" +
            "      \"must\": [\n" +
            "        { \"match\": { \"age\": \"40\" } }\n" +
            "      ],\n" +
            "      \"must_not\": [\n" +
            "        { \"match\": { \"state\": \"ID\" } }\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";

        HttpEntity entity = new NStringEntity(lowLevelRequestBody, ContentType.APPLICATION_JSON);
        Map<String, String> params = Collections.singletonMap("pretty", "true");

        Response response = lowLevelClient.performRequest("GET", "/bank/_search", params, entity);
        Map<String, Object> responseBody = readBody(response.getEntity().getContent());

        assert response.getStatusLine().getStatusCode() == 200;

        Map<String, Object> hits = (Map<String, Object>) responseBody.get("hits");
        Double total = (Double) hits.get("total");
        assert total == 43;
    }

    @Test
    public void test5Filtering() throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.matchAllQuery())
                .filter(QueryBuilders.rangeQuery("balance").gte(20000).lte(30000))
        );

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = highLevelClient.search(searchRequest);

        assert searchResponse.getHits().getTotalHits() == 217;

        String lowLevelRequestBody = "{\n" +
            "  \"query\": {\n" +
            "    \"bool\": {\n" +
            "      \"must\": { \"match_all\": {} },\n" +
            "      \"filter\": {\n" +
            "        \"range\": {\n" +
            "          \"balance\": {\n" +
            "            \"gte\": 20000,\n" +
            "            \"lte\": 30000\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";

        HttpEntity entity = new NStringEntity(lowLevelRequestBody, ContentType.APPLICATION_JSON);
        Map<String, String> params = Collections.singletonMap("pretty", "true");

        Response response = lowLevelClient.performRequest("GET", "/bank/_search", params, entity);
        Map<String, Object> responseBody = readBody(response.getEntity().getContent());

        assert response.getStatusLine().getStatusCode() == 200;

        Map<String, Object> hits = (Map<String, Object>) responseBody.get("hits");
        Double total = (Double) hits.get("total");
        assert total == 217;
    }



    @Test
    public void test6Aggregations() throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0)
            .aggregation(AggregationBuilders.terms("group_by_state").field("state.keyword"));

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = highLevelClient.search(searchRequest);

        ParsedStringTerms groupByState = (ParsedStringTerms) searchResponse.getAggregations().get("group_by_state");

        assert groupByState.getDocCountError() == 20;
        assert groupByState.getSumOfOtherDocCounts() == 770;

        String lowLevelRequestBody = "{\n" +
            "  \"size\": 0,\n" +
            "  \"aggs\": {\n" +
            "    \"group_by_state\": {\n" +
            "      \"terms\": {\n" +
            "        \"field\": \"state.keyword\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";

        HttpEntity entity = new NStringEntity(lowLevelRequestBody, ContentType.APPLICATION_JSON);
        Map<String, String> params = Collections.singletonMap("pretty", "true");

        Response response = lowLevelClient.performRequest("GET", "/bank/_search", params, entity);
        Map<String, Object> responseBody = readBody(response.getEntity().getContent());

        assert response.getStatusLine().getStatusCode() == 200;

        Map<String, Object> aggregations = (Map<String, Object>) responseBody.get("aggregations");
        Map<String, Object> byStates = (Map<String, Object>) aggregations.get("group_by_state");
        Double docCountErrorUpperBound = (Double) byStates.get("doc_count_error_upper_bound");
        Double sumOtherDocCount = (Double) byStates.get("sum_other_doc_count");
        assert docCountErrorUpperBound == 20;
        assert sumOtherDocCount == 770;
    }

    @Test
    public void test7DeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("bank");

        //It is possible to do in a async request, but for test it is not necessary.
        DeleteIndexResponse deleteIndexResponse = highLevelClient.indices().delete(request);

        assert deleteIndexResponse.isAcknowledged();
    }

    private String readString(InputStream inputStream) {
        BufferedReader buffer = new BufferedReader(new InputStreamReader(inputStream));

        return buffer.lines().collect(Collectors.joining("\n"));
    }

    private Map<String, Object> readBody(InputStream inputStream) {
        String body = readString(inputStream);

        Gson gson = new Gson();

        return gson.fromJson(body, HashMap.class);
    }
}