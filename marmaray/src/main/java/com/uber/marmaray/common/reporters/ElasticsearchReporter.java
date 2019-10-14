package com.uber.marmaray.common.reporters;

import com.uber.marmaray.common.metrics.Metric;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.Handler;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Elasticsearch implementation of {@link IReporter}
 */
@Slf4j
public class ElasticsearchReporter implements IReporter<Metric> {
    final private String serverURL;
    final private int serverPort;
    final private BulkRequest request;
    final private String indexName;

    public ElasticsearchReporter(@NonNull final String serverURL, @NonNull final int serverPort,
                                 @NonNull final String indexName) {
        this.serverURL = serverURL;
        this.serverPort = serverPort;
        this.request = new BulkRequest();
        this.request.timeout(TimeValue.timeValueSeconds(10));
        this.indexName = indexName;
    }

    public void gauge(@NonNull final Metric m) {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("name", m.getMetricName());
        jsonMap.put("value", Long.parseLong(m.getMetricValue().toString()));
        jsonMap.put("tags", m.getTags());
        jsonMap.put("postDate", new Date());
        IndexRequest indexRequest = new IndexRequest(this.indexName).source(jsonMap).type("_doc");

        this.request.add(indexRequest);
    }

    public void finish() {
        final RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(serverURL, serverPort, "http")
                )
        );

        try {
            BulkResponse bulkResponse = client.bulk(this.request, RequestOptions.DEFAULT);
            if (bulkResponse.hasFailures()) {
                for (BulkItemResponse bulkItemResponse : bulkResponse) {
                    if (bulkItemResponse.isFailed()) {
                        BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                        log.error(failure.toString());
                    }
                }
            }
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}