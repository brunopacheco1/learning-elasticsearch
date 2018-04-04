package com.dev.bruno.learning.elasticsearch;

import com.google.gson.Gson;
import org.apache.http.HttpHost;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class ClusterTest {

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
    public void cluster() throws IOException {
        //There are no High Level API to check the cluster's health
        Response response = lowLevelClient.performRequest("GET", "/_cat/health");
        String responseBody = readString(response.getEntity().getContent());

        assert response.getStatusLine().getStatusCode() == 200;
        assert responseBody.contains("green");
    }

    @Test
    public void nodes() throws IOException {
        //There are no High Level API to list all nodes
        Response response = lowLevelClient.performRequest("GET", "/_cat/nodes");
        assert response.getStatusLine().getStatusCode() == 200;

        //You can have some info about the cluster and the node which handled the request
        MainResponse info = highLevelClient.info();

        assert info.isAvailable();
    }

    private String readString(InputStream inputStream) {
        BufferedReader buffer = new BufferedReader(new InputStreamReader(inputStream));

        return buffer.lines().collect(Collectors.joining("\n"));
    }
}
