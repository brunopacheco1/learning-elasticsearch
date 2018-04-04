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
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DocumentsTest {

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
        CreateIndexRequest request = new CreateIndexRequest("customer");
        request.settings(Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
        );

        request.mapping("_doc", "name", "type=text");

        //It is possible to do in a async request, but for test it is not necessary.
        CreateIndexResponse createIndexResponse = highLevelClient.indices().create(request);

        assert createIndexResponse.isShardsAcknowledged();
        assert createIndexResponse.isAcknowledged();

        String requestBody = "{\n" +
            "    \"settings\" : {\n" +
            "        \"number_of_shards\" : 1,\n" +
            "\t\t\"number_of_replicas\" : 0\n" +
            "    },\n" +
            "    \"mappings\" : {\n" +
            "        \"_doc\" : {\n" +
            "            \"properties\" : {\n" +
            "                \"name\" : { \"type\" : \"text\" }\n" +
            "            }\n" +
            "        }\n" +
            "    }\n" +
            "}";

        HttpEntity entity = new NStringEntity(requestBody, ContentType.APPLICATION_JSON);
        Map<String, String> params = Collections.singletonMap("pretty", "true");

        Response response = lowLevelClient.performRequest("PUT", "/customer2", params, entity);
        Map<String, Object> responseBody = readBody(response.getEntity().getContent());

        assert response.getStatusLine().getStatusCode() == 200;
        assert (Boolean) responseBody.get("acknowledged");
        assert (Boolean) responseBody.get("shards_acknowledged");
    }

    @Test
    public void test2CreateDocument() throws IOException {
        IndexRequest request = new IndexRequest(
            "customer",
            "_doc",
            "1");
        String requestBody = "{ \"name\":\"John Doe\" }";
        request.source(requestBody, XContentType.JSON);
        request.create(true);

        IndexResponse indexResponse = highLevelClient.index(request);

        assert indexResponse.status().equals(RestStatus.CREATED);

        HttpEntity entity = new NStringEntity(requestBody, ContentType.APPLICATION_JSON);
        Map<String, String> params = Collections.singletonMap("pretty", "true");

        Response response = lowLevelClient.performRequest("PUT", "/customer2/_doc/1", params, entity);

        assert response.getStatusLine().getStatusCode() == 201;
    }

    @Test
    public void test3GetDocument() throws IOException {
        GetRequest request = new GetRequest("customer", "_doc", "1");

        GetResponse getResponse = highLevelClient.get(request);

        assert getResponse.isExists();
        assert getResponse.getSourceAsMap().get("name").equals("John Doe");

        Response response = lowLevelClient.performRequest("GET", "/customer2/_doc/1");
        Map<String, Object> responseBody = readBody(response.getEntity().getContent());

        assert response.getStatusLine().getStatusCode() == 200;
        assert (Boolean) responseBody.get("found");

        Map<String, Object> customer = (Map<String, Object>) responseBody.get("_source");
        assert customer.get("name").equals("John Doe");
    }

    @Test
    public void test4UpdateDocument() throws IOException {
        UpdateRequest request = new UpdateRequest(
            "customer",
            "_doc",
            "1");
        String requestBody = "{ \"name\":\"John Doe Update\" }";

        request.doc(requestBody, XContentType.JSON);

        UpdateResponse updateResponse = highLevelClient.update(request);

        assert updateResponse.status().equals(RestStatus.OK);

        String lowLevelRequestBody = "{ \"doc\" : { \"name\":\"John Doe Update\" }}";

        HttpEntity entity = new NStringEntity(lowLevelRequestBody, ContentType.APPLICATION_JSON);
        Map<String, String> params = Collections.singletonMap("pretty", "true");

        Response response = lowLevelClient.performRequest("POST", "/customer2/_doc/1/_update", params, entity);

        assert response.getStatusLine().getStatusCode() == 200;
    }

    @Test
    public void test5GetDocumentAfterUpdate() throws IOException {
        GetRequest request = new GetRequest("customer", "_doc", "1");

        GetResponse getResponse = highLevelClient.get(request);

        assert getResponse.isExists();
        assert getResponse.getSourceAsMap().get("name").equals("John Doe Update");

        Response response = lowLevelClient.performRequest("GET", "/customer2/_doc/1");
        Map<String, Object> responseBody = readBody(response.getEntity().getContent());

        assert response.getStatusLine().getStatusCode() == 200;
        assert (Boolean) responseBody.get("found");

        Map<String, Object> customer = (Map<String, Object>) responseBody.get("_source");
        assert customer.get("name").equals("John Doe Update");
    }

    @Test
    public void test6DeleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest("customer", "_doc", "1");

        DeleteResponse deleteResponse = highLevelClient.delete(request);

        RestStatus status = deleteResponse.status();

        assert deleteResponse.status().equals(RestStatus.OK);

        Response response = lowLevelClient.performRequest("DELETE", "/customer2/_doc/1");
        Map<String, Object> responseBody = readBody(response.getEntity().getContent());

        assert response.getStatusLine().getStatusCode() == 200;
        assert responseBody.get("result").toString().equals("deleted");
    }

    @Test
    public void test7BulkOperations() throws IOException {
        IndexRequest indexRequest = new IndexRequest(
            "customer",
            "_doc",
            "2");
        String indexRequestBody = "{ \"name\":\"John Doe\" }";
        indexRequest.source(indexRequestBody, XContentType.JSON);
        indexRequest.create(true);

        UpdateRequest updateRequest = new UpdateRequest(
            "customer",
            "_doc",
            "2");
        String updateRequestBody = "{ \"name\":\"John Doe Update\" }";
        updateRequest.doc(updateRequestBody, XContentType.JSON);

        DeleteRequest deleteRequest = new DeleteRequest("customer", "_doc", "2");

        BulkRequest request = new BulkRequest();
        request.add(indexRequest);
        request.add(updateRequest);
        request.add(deleteRequest);

        BulkResponse bulkResponse = highLevelClient.bulk(request);
        assert bulkResponse.status().equals(RestStatus.OK);
        assert !bulkResponse.hasFailures();

        String lowLevelRequestBody = "{\"index\":{\"_id\":\"1\"}}\n" +
            "{\"name\": \"John Doe\" }\n" +
            "{\"update\":{\"_id\":\"1\"}}\n" +
            "{\"doc\": { \"name\": \"John Doe becomes Jane Doe\" } }\n" +
            "{\"delete\":{\"_id\":\"1\"}}\n";

        HttpEntity entity = new NStringEntity(lowLevelRequestBody, ContentType.APPLICATION_JSON);
        Map<String, String> params = Collections.singletonMap("pretty", "true");

        Response response = lowLevelClient.performRequest("POST", "/customer2/_doc/_bulk", params, entity);
        Map<String, Object> responseBody = readBody(response.getEntity().getContent());

        assert response.getStatusLine().getStatusCode() == 200;
        assert !((Boolean) responseBody.get("errors"));
    }

    @Test
    public void test8DeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("customer");

        //It is possible to do in a async request, but for test it is not necessary.
        DeleteIndexResponse deleteIndexResponse = highLevelClient.indices().delete(request);

        assert deleteIndexResponse.isAcknowledged();

        Response response = lowLevelClient.performRequest("DELETE", "/customer2");
        Map<String, Object> responseBody = readBody(response.getEntity().getContent());

        assert response.getStatusLine().getStatusCode() == 200;
        assert (Boolean) responseBody.get("acknowledged");
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