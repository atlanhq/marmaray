package com.uber.marmaray.common.reporters;

import com.uber.marmaray.common.metrics.Metric;

import com.uber.marmaray.common.configuration.Configuration;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.HashMap;

@Slf4j
public class PrometheusReporter implements IReporter<Metric> {
    public static PushGateway pg;
    public static Configuration config;
    public final CollectorRegistry registry = new CollectorRegistry();
    public static HashMap<String, Gauge> metricsMap = new HashMap<>();
    public PrometheusReporter(@NonNull final Configuration conf) {
        pg = new PushGateway("127.0.0.1:9091");
        config = conf;
    }
    @Override
    public void gauge(Metric m) {
        final String metricName = m.getMetricName();
        final String metricValue = m.getMetricValue().toString();
        final String tags = m.getTags().toString();
        final String tableName = config.getProperty("marmaray.hoodie.tables.test_hoodie.table_name").get();

        log.info("{}={}, Tags: {}", metricName, metricValue, tags);
        Gauge metric = metricsMap.get(metricName);
        if (metric == null) {
            metric = Gauge.build().name(metricName).help("Not Available").labelNames("table_name")
                    .register(registry);
            metricsMap.put(metricName, metric);
        }
        metric.labels(tableName).set(Double.parseDouble(metricValue));
        try {
            pg.pushAdd(registry, "Marmaray");
        } catch (IOException e) {
            log.error("-----------------------------------------");
            log.error("+==============Error in pushing fvcv=============");
            e.printStackTrace();
        }
    }

    @Override
    public void finish() {

    }
}
