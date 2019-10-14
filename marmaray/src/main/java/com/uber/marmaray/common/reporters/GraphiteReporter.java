package com.uber.marmaray.common.reporters;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.uber.marmaray.common.metrics.Metric;
import com.uber.marmaray.common.reporters.IReporter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Graphite implementation of {@link IReporter}
 */
@Slf4j
public class GraphiteReporter implements IReporter<Metric> {

    private final Graphite graphite;
    private final MetricRegistry metricRegistry;
    private final com.codahale.metrics.graphite.GraphiteReporter reporter;

    public GraphiteReporter(final String serverURL, final Integer serverPort) {
        this.graphite = new Graphite((new InetSocketAddress(serverURL, serverPort)));
        this.metricRegistry = new MetricRegistry();
        this.reporter = com.codahale.metrics.graphite.GraphiteReporter.forRegistry(metricRegistry)
                .prefixedWith("mymetrics.requests")
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .build(graphite);
    }

    public void gauge(@NonNull final Metric m) {
        long metricValue = Long.parseLong(m.getMetricValue().toString());
        this.metricRegistry.register(m.getMetricName(), new Gauge<Long>() {
            @Override
            public Long getValue() {
                return metricValue;
            }
        });
    }

    public void finish() {
        this.reporter.report();
    }
}
