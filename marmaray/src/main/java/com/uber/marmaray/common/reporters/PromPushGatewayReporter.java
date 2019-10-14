package com.uber.marmaray.common.reporters;

import com.uber.marmaray.common.metrics.Metric;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

@Slf4j
public class PromPushGatewayReporter implements IReporter<Metric> {
    private final CollectorRegistry collectorRegistry;
    private final PushGateway pushGateway;
    private final String jobName;

    public PromPushGatewayReporter(final String pushGatewayURL, final String jobName) {
        this.collectorRegistry = new CollectorRegistry();
        this.pushGateway = new PushGateway(pushGatewayURL);
        this.jobName = jobName;
    }

    public void gauge(@NonNull final Metric m) {
        final Map<String, String> tags = m.getTags();
        Gauge gauge = Gauge.build().name(m.getMetricName()).help(m.getMetricName())
                .labelNames(cleanedPromString(tags.keySet().toArray(new String[0])))
                .register(this.collectorRegistry);

        long metricValue = Long.parseLong(m.getMetricValue().toString());
        gauge.labels(tags.values().toArray(new String[0])).set(metricValue);
    }

    private String[] cleanedPromString(@NonNull String... strings) {
        ArrayList<String> cleanedStrings = new ArrayList<>();
        for (String s: strings) {
            cleanedStrings.add(s.replace("-", "_"));
        }
        return cleanedStrings.toArray(new String[0]);
    }

    public void finish() {
        try {
            this.pushGateway.push(this.collectorRegistry, this.jobName);
        }
        catch (IOException e) {
            log.error(e.toString());
        }
    }
}
