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
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
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
public class IndicesTest {

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
    public void test2ExistIndices() throws IOException {
        //There is not high level api to check if an index exists in the version 6.2

        //404 for not existing index
        Response response = lowLevelClient.performRequest("HEAD", "/customer2");
        assert response.getStatusLine().getStatusCode() == 200;
    }

    @Test
    public void test3DeleteIndex() throws IOException {
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
