package com.dev.bruno.learning.elasticsearch;

import com.google.gson.Gson;
import com.vividsolutions.jts.geom.Coordinate;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilders;
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
public class GeospatialSearchTest {

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
        CreateIndexRequest request = new CreateIndexRequest("example");
        request.settings(Settings.builder()
            .put("index.number_of_shards", 5)
            .put("index.number_of_replicas", 0)
        );

        request.mapping("_doc", "location", "type=geo_shape");

        CreateIndexResponse createIndexResponse = highLevelClient.indices().create(request);

        assert createIndexResponse.isShardsAcknowledged();
        assert createIndexResponse.isAcknowledged();
    }

    @Test
    public void test2CreateDocument() throws IOException, InterruptedException {
        IndexRequest request = new IndexRequest(
                "example",
                "_doc",
                "1");

        String requestBody = "{\n" +
                "    \"name\": \"Wind & Wetter, Berlin, Germany\",\n" +
                "    \"location\": {\n" +
                "        \"type\": \"point\",\n" +
                "        \"coordinates\": [13.400544, 52.530286]\n" +
                "    }\n" +
                "}";

        request.source(requestBody, XContentType.JSON);
        request.create(true);

        IndexResponse indexResponse = highLevelClient.index(request);

        assert indexResponse.status().equals(RestStatus.CREATED);

        //Bulk is a async request
        Thread.sleep(1000L);
    }

    @Test
    public void test3GeoQuery() throws IOException {
        EnvelopeBuilder envelopeBuilder = new EnvelopeBuilder(new Coordinate(13.0, 53.0) , new Coordinate(14.0, 52.0 ));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.matchAllQuery())
                .filter(QueryBuilders.geoShapeQuery("location", envelopeBuilder).relation(ShapeRelation.WITHIN))
        );

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = highLevelClient.search(searchRequest);

        assert searchResponse.getHits().getTotalHits() == 1;

        String lowLevelRequestBody = "{\n" +
                "    \"query\":{\n" +
                "        \"bool\": {\n" +
                "            \"must\": {\n" +
                "                \"match_all\": {}\n" +
                "            },\n" +
                "            \"filter\": {\n" +
                "                \"geo_shape\": {\n" +
                "                    \"location\": {\n" +
                "                        \"shape\": {\n" +
                "                            \"type\": \"envelope\",\n" +
                "                            \"coordinates\" : [[13.0, 53.0], [14.0, 52.0]]\n" +
                "                        },\n" +
                "                        \"relation\": \"within\"\n" +
                "                    }\n" +
                "                }\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";

        HttpEntity entity = new NStringEntity(lowLevelRequestBody, ContentType.APPLICATION_JSON);
        Map<String, String> params = Collections.singletonMap("pretty", "true");

        Response response = lowLevelClient.performRequest("GET", "/example/_search", params, entity);
        Map<String, Object> responseBody = readBody(response.getEntity().getContent());

        assert response.getStatusLine().getStatusCode() == 200;

        Map<String, Object> hits = (Map<String, Object>) responseBody.get("hits");
        Double total = (Double) hits.get("total");
        assert total == 1;
    }

    @Test
    public void test7DeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("example");

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