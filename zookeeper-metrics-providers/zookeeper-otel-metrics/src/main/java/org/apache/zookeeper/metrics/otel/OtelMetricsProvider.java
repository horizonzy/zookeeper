/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.metrics.otel;

import io.netty.util.internal.PlatformDependent;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributeType;
import io.opentelemetry.api.internal.InternalAttributeKeyImpl;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.instrumentation.runtimemetrics.BufferPools;
import io.opentelemetry.instrumentation.runtimemetrics.Classes;
import io.opentelemetry.instrumentation.runtimemetrics.Cpu;
import io.opentelemetry.instrumentation.runtimemetrics.GarbageCollector;
import io.opentelemetry.instrumentation.runtimemetrics.MemoryPools;
import io.opentelemetry.instrumentation.runtimemetrics.Threads;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentelemetry.sdk.internal.AttributesMap;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.View;
import org.apache.zookeeper.common.StringUtils;
import org.apache.zookeeper.metrics.Counter;
import org.apache.zookeeper.metrics.CounterSet;
import org.apache.zookeeper.metrics.Gauge;
import org.apache.zookeeper.metrics.GaugeSet;
import org.apache.zookeeper.metrics.MetricsContext;
import org.apache.zookeeper.metrics.MetricsProvider;
import org.apache.zookeeper.metrics.MetricsProviderLifeCycleException;
import org.apache.zookeeper.metrics.Summary;
import org.apache.zookeeper.metrics.SummarySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class OtelMetricsProvider implements MetricsProvider {

    private static final Logger LOG = LoggerFactory.getLogger(OtelMetricsProvider.class);

    private static final String ROOT_NAME = "org.apache.zookeeper";

    private static final String TIME = "time";

    private static final String LENGTH = "length";

    private static final String BYTES = "bytes";

    private static final String CUSTOM_METRICS_PREFIX = "otel.metrics.unit.";

    private final Map<String, String> metricsUnit = new HashMap<>();
    
    {
        metricsUnit.put("fsynctime", TIME);
        metricsUnit.put("snapshottime", TIME);
        metricsUnit.put("restore_time", TIME);
        metricsUnit.put("dbinittime", TIME);
        metricsUnit.put("readlatency", TIME);
        metricsUnit.put("updatelatency", TIME);
        metricsUnit.put("propagation_latency", TIME);
        metricsUnit.put("follower_sync_time", TIME);
        metricsUnit.put("observer_sync_time", TIME);
        metricsUnit.put("election_time", TIME);
        metricsUnit.put("dead_watchers_cleaner_latency", TIME);
        metricsUnit.put("prep_processor_queue_time_ms", TIME);
        metricsUnit.put("prep_process_time", TIME);
        metricsUnit.put("proposal_process_time", TIME);
        metricsUnit.put("close_session_prep_time", TIME);
        metricsUnit.put("unavailable_time", TIME);
        metricsUnit.put("leader_unavailable_time", TIME);
        metricsUnit.put("time_waiting_empty_pool_in_commit_processor_read_ms", TIME);
        metricsUnit.put("write_batch_time_in_commit_processor", TIME);
        metricsUnit.put("read_commitproc_time_ms", TIME);
        metricsUnit.put("write_commitproc_time_ms", TIME);
        metricsUnit.put("local_write_committed_time_ms", TIME);
        metricsUnit.put("server_write_committed_time_ms", TIME);
        metricsUnit.put("commit_process_time", TIME);
        metricsUnit.put("om_proposal_process_time_ms", TIME);
        metricsUnit.put("om_commit_process_time_ms", TIME);
        metricsUnit.put("read_final_proc_time_ms", TIME);
        metricsUnit.put("write_final_proc_time_ms", TIME);
        metricsUnit.put("proposal_latency", TIME);
        metricsUnit.put("proposal_ack_creation_latency", TIME);
        metricsUnit.put("commit_propagation_latency", TIME);
        metricsUnit.put("learner_handler_qp_time_ms", TIME);
        metricsUnit.put("startup_txns_load_time", TIME);
        metricsUnit.put("startup_snap_load_time", TIME);
        metricsUnit.put("sync_processor_queue_and_flush_time_ms", TIME);
        metricsUnit.put("sync_processor_queue_time_ms", TIME);
        metricsUnit.put("sync_processor_queue_flush_time_ms", TIME);
        metricsUnit.put("sync_process_time", TIME);
        metricsUnit.put("quorum_ack_latency", TIME);
        metricsUnit.put("ack_latency", TIME);
        metricsUnit.put("request_throttle_queue_time_ms", TIME);
        metricsUnit.put("socket_closing_time", TIME);
    }

    private static final List<Double> timeBuckets = Arrays.asList(
            0.1, 0.2, 0.5,
            1.0, 2.0, 5.0,
            10.0, 20.0, 50.0,
            100.0, 200.0, 500.0,
            1_000.0, 2_000.0, 5_000.0,
            10_000.0, 20_000.0, 50_000.0
    );

    {
        metricsUnit.put("connection_token_deficit", LENGTH);
        metricsUnit.put("inflight_snap_count", LENGTH);
        metricsUnit.put("inflight_diff_count", LENGTH);
        metricsUnit.put("node_created_watch_count", LENGTH);
        metricsUnit.put("node_deleted_watch_count", LENGTH);
        metricsUnit.put("node_changed_watch_count", LENGTH);
        metricsUnit.put("node_children_watch_count", LENGTH);
        metricsUnit.put("prep_processor_queue_size", LENGTH);
        metricsUnit.put("requests_in_session_queue", LENGTH);
        metricsUnit.put("pending_session_queue_size", LENGTH);
        metricsUnit.put("reads_after_write_in_session_queue", LENGTH);
        metricsUnit.put("reads_issued_from_session_queue", LENGTH);
        metricsUnit.put("session_queues_drained", LENGTH);
        metricsUnit.put("concurrent_request_processing_in_commit_processor", LENGTH);
        metricsUnit.put("read_commit_proc_req_queued", LENGTH);
        metricsUnit.put("write_commit_proc_req_queued", LENGTH);
        metricsUnit.put("commit_commit_proc_req_queued", LENGTH);
        metricsUnit.put("read_commit_proc_issued", LENGTH);
        metricsUnit.put("write_commit_proc_issued", LENGTH);
        metricsUnit.put("learner_handler_qp_size", LENGTH);
        metricsUnit.put("startup_txns_loaded", LENGTH);
        metricsUnit.put("sync_processor_queue_size", LENGTH);
        metricsUnit.put("sync_processor_batch_size", LENGTH);
        metricsUnit.put("learner_request_processor_queue_size", LENGTH);
    }

    private static final List<Double> lengthBuckets = Arrays.asList(
            1.0, 2.0, 5.0,
            10.0, 20.0, 50.0,
            100.0, 200.0, 500.0,
            1_000.0, 2_000.0, 5_000.0,
            10_000.0, 50_000.0, 100_000.0
    );

    {
        metricsUnit.put("write_per_namespace", BYTES);
        metricsUnit.put("read_per_namespace", BYTES);
        metricsUnit.put("netty_queued_buffer_capacity", BYTES);
    }

    private static final List<Double> bytesBuckets = Arrays.asList(
            1.0, 2.0, 4.0,
            8.0, 16.0, 32.0,
            64.0, 128.0, 256.0,
            512.0, 1024.0, 2048.0,
            4096.0, 8192.0, 16384.0,
            32768.0, 65536.0, 131072.0,
            262144.0, 524288.0, 1048576.0,
            2097152.0, 4194304.0, 8388608.0,
            16777216.0, 33554432.0, 67108864.0,
            134217728.0, 268435456.0, 536870912.0,
            1073741824.0
    );

    private final OpenTelemetry openTelemetry;

    private final Meter meter;

    private boolean exportJvmInfo = true;

    private final OtelContext rootContext;

    public OtelMetricsProvider() {
        AutoConfiguredOpenTelemetrySdk sdk = AutoConfiguredOpenTelemetrySdk.builder()
                .addMeterProviderCustomizer(
                        (sdkMeterProviderBuilder, configProperties) -> {
                            sdkMeterProviderBuilder.registerView(
                                    InstrumentSelector.builder()
                                            .setMeterName(ROOT_NAME)
                                            .setUnit(TIME)
                                            .setType(InstrumentType.HISTOGRAM)
                                            .build(),
                                    View.builder()
                                            .setAggregation(Aggregation.explicitBucketHistogram(timeBuckets))
                                            .build());
                            sdkMeterProviderBuilder.registerView(
                                    InstrumentSelector.builder()
                                            .setMeterName(ROOT_NAME)
                                            .setUnit(LENGTH)
                                            .setType(InstrumentType.HISTOGRAM)
                                            .build(),
                                    View.builder()
                                            .setAggregation(Aggregation.explicitBucketHistogram(lengthBuckets))
                                            .build());
                            sdkMeterProviderBuilder.registerView(
                                    InstrumentSelector.builder()
                                            .setMeterName(ROOT_NAME)
                                            .setUnit(BYTES)
                                            .setType(InstrumentType.HISTOGRAM)
                                            .build(),
                                    View.builder()
                                            .setAggregation(Aggregation.explicitBucketHistogram(bytesBuckets))
                                            .build());
                            return sdkMeterProviderBuilder;
                        }

                ).build();
        this.openTelemetry = sdk.getOpenTelemetrySdk();
        this.meter = openTelemetry.getMeter(ROOT_NAME);
        rootContext = new OtelContext(meter, "");
    }


    @Override
    public void configure(Properties configuration) throws MetricsProviderLifeCycleException {
        this.exportJvmInfo = Boolean.parseBoolean(configuration.getProperty("exportJvmInfo", "true"));
        configuration.forEach((k, v) -> {
            if (k instanceof String && ((String) k).startsWith(CUSTOM_METRICS_PREFIX)) {
                metricsUnit.put(((String) k).substring(CUSTOM_METRICS_PREFIX.length()), (String) v);
            }
        });
    }

    @Override
    public void start() throws MetricsProviderLifeCycleException {
        if (exportJvmInfo) {
            // Include standard JVM stats
            MemoryPools.registerObservers(openTelemetry);
            BufferPools.registerObservers(openTelemetry);
            Classes.registerObservers(openTelemetry);
            Cpu.registerObservers(openTelemetry);
            Threads.registerObservers(openTelemetry);
            GarbageCollector.registerObservers(openTelemetry);

            meter.gaugeBuilder("process.runtime.jvm.memory.direct_bytes_used")
                    .buildWithCallback(odm -> odm.record(getDirectMemoryUsage.get()));

            meter.gaugeBuilder("process.runtime.jvm.memory.direct_bytes_max")
                    .buildWithCallback(odm -> odm.record(PlatformDependent.estimateMaxDirectMemory()));
        }
    }

    @Override
    public MetricsContext getRootContext() {
        return rootContext;
    }

    @Override
    public void stop() {
    }

    @Override
    public void dump(BiConsumer<String, Object> sink) {

    }

    @Override
    public void resetAllValues() {
        // not supported on otel
    }

    private class OtelContext implements MetricsContext {

        private final String name;
        private final Meter meter;

        private final ConcurrentMap<String, OtelGaugeWrapper> gauges = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, OtelLabelledGaugeWrapper> gaugeSets = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, OtelCounter> counters = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, OtelCounterSet> counterSets = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, OtelSummary> summaries = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, OtelLableledSummary> summarySets = new ConcurrentHashMap<>();

        private OtelContext(Meter meter, String name) {
            this.meter = meter;
            this.name = name;
        }

        @Override
        public MetricsContext getContext(String name) {
            return new OtelContext(meter, completeName(name));
        }

        private String completeName(String subName) {
            String metricName = name.isEmpty()
                    ? subName
                    : new StringJoiner(".").add(name).add(subName).toString();
            return metricName;
        }

        @Override
        public Counter getCounter(String name) {
            return counters.computeIfAbsent(completeName(name), k -> new OtelCounter(meter, name));
        }


        @Override
        public CounterSet getCounterSet(final String name) {
            Objects.requireNonNull(name, "Cannot register a CounterSet with null name");
            return counterSets.computeIfAbsent(completeName(name), k -> new OtelCounterSet(meter, name));
        }

        /**
         * Gauges may go up and down, in ZooKeeper they are a way to export
         * internal values with a callback.
         *
         * @param name  the name of the gauge
         * @param gauge the callback
         */
        @Override
        public void registerGauge(String name, Gauge gauge) {
            Objects.requireNonNull(name);
            gauges.compute(completeName(name), (id, prev) ->
                    new OtelGaugeWrapper(meter, id, gauge, prev != null ? prev.inner : null));
        }

        @Override
        public void unregisterGauge(String name) {
            OtelGaugeWrapper existing = gauges.remove(completeName(name));
            if (existing != null) {
                existing.unregister();
            }
        }

        @Override
        public void registerGaugeSet(final String name, final GaugeSet gaugeSet) {
            Objects.requireNonNull(name, "Cannot register a GaugeSet with null name");
            Objects.requireNonNull(gaugeSet, "Cannot register a null GaugeSet for " + name);
            gaugeSets.compute(completeName(name), (id, prev) ->
                    new OtelLabelledGaugeWrapper(meter, completeName(name), gaugeSet, prev != null ? prev.inner : null));
        }

        @Override
        public void unregisterGaugeSet(final String name) {
            Objects.requireNonNull(name, "Cannot unregister GaugeSet with null name");
            final OtelLabelledGaugeWrapper existing = gaugeSets.remove(completeName(name));
            if (existing != null) {
                existing.unregister();
            }
        }

        @Override
        public Summary getSummary(String name, DetailLevel detailLevel) {
            return summaries.compute(completeName(name), (k, v) -> {
                if (v != null) {
                    throw new IllegalArgumentException("Already registered a non basic summary as " + k);
                }
                return new OtelSummary(meter, completeName(name), getUnit(name));
            });
        }

        @Override
        public SummarySet getSummarySet(String name, DetailLevel detailLevel) {
            return summarySets.computeIfAbsent(completeName(name), (n) -> {
                if (summarySets.containsKey(n)) {
                    throw new IllegalArgumentException("Already registered a basic summary set as " + n);
                }
                return new OtelLableledSummary(meter, completeName(name), getUnit(name));
            });
        }
    }

    private String getUnit(String name) {
        String unit = metricsUnit.get(name);
        if (StringUtils.isEmpty(unit)) {
            //Use time as the default unit.
            return TIME;
        }
        if (!TIME.equals(unit) && !LENGTH.equals(unit) && !BYTES.equals(unit)) {
            LOG.warn("Unknown metrics unit: {}", name);
            return TIME;
        }
        return unit;
    }

    private AttributesMap buildAttributeMap(String key) {
        AttributesMap map = AttributesMap.create(1, 1);
        AttributeKey<String> ak = InternalAttributeKeyImpl.create("key", AttributeType.STRING);
        map.put(ak, key);
        return map;
    }

    private class OtelCounter implements Counter {

        private final LongUpDownCounter counter;

        OtelCounter(Meter meter, String name) {
            this.counter = meter.upDownCounterBuilder(name).build();
        }

        @Override
        public void inc() {
            counter.add(1);
        }

        @Override
        public void add(long delta) {
            counter.add(delta);
        }

        @Override
        public long get() {
            return -1L;
        }
    }

    private class OtelCounterSet implements CounterSet {

        private final LongUpDownCounter counter;

        OtelCounterSet(Meter meter, String name) {
            this.counter = meter.upDownCounterBuilder(name).build();
        }

        @Override
        public void add(String key, long delta) {
            AttributesMap attribute = buildAttributeMap(key);
            counter.add(delta, attribute);
        }
    }

    private class OtelGaugeWrapper {

        private final ObservableDoubleGauge inner;

        public OtelGaugeWrapper(Meter meter, String name, Gauge gauge, ObservableDoubleGauge prev) {
            this.inner = prev != null ? prev
                    : meter.gaugeBuilder(name)
                    .buildWithCallback(observableDoubleMeasurement -> {
                        double v = gauge.get().doubleValue();
                        observableDoubleMeasurement.record(v);
                    });
        }

        private void unregister() {
            inner.close();
        }
    }

    private class OtelLabelledGaugeWrapper {

        private final ObservableDoubleGauge inner;

        public OtelLabelledGaugeWrapper(Meter meter, String name, GaugeSet gaugeSet, ObservableDoubleGauge prev) {
            this.inner = prev != null ? prev
                    : meter.gaugeBuilder(name)
                    .buildWithCallback(observableDoubleMeasurement -> {
                        gaugeSet.values().forEach((key, value) -> {
                            AttributesMap attribute = buildAttributeMap(key);
                            observableDoubleMeasurement.record(value != null ? value.doubleValue() : 0, attribute);
                        });
                    });
        }

        private void unregister() {
            inner.close();
        }
    }

    private class OtelSummary implements Summary {

        private final DoubleHistogram histogram;

        public OtelSummary(Meter meter, String name, String unit) {
            histogram = meter.histogramBuilder(name).setUnit(unit).build();
        }

        @Override
        public void add(long value) {
            histogram.record(value);
        }
    }

    private class OtelLableledSummary implements SummarySet {

        private final DoubleHistogram histogram;

        public OtelLableledSummary(Meter meter, String name, String unit) {
            histogram = meter.histogramBuilder(name).setUnit(unit).build();
        }

        @Override
        public void add(String key, long value) {
            AttributesMap attributes = buildAttributeMap(key);
            histogram.record(value, attributes);
        }
    }

    /*
     * Try to get Netty counter of used direct memory. This will be correct, unlike the JVM values.
     */
    private static final AtomicLong directMemoryUsage;
    private static final Optional<BufferPoolMXBean> poolMxBeanOp;
    private static final Supplier<Double> getDirectMemoryUsage;

    static {
        if (PlatformDependent.useDirectBufferNoCleaner()) {
            poolMxBeanOp = Optional.empty();
            AtomicLong tmpDirectMemoryUsage = null;
            try {
                Field field = PlatformDependent.class.getDeclaredField("DIRECT_MEMORY_COUNTER");
                field.setAccessible(true);
                tmpDirectMemoryUsage = (AtomicLong) field.get(null);
            } catch (Throwable t) {
                LOG.warn("Failed to access netty DIRECT_MEMORY_COUNTER field {}", t.getMessage());
            }
            directMemoryUsage = tmpDirectMemoryUsage;
            getDirectMemoryUsage = () -> directMemoryUsage != null ? directMemoryUsage.get() : Double.NaN;
        } else {
            directMemoryUsage = null;
            List<BufferPoolMXBean> platformMXBeans = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
            poolMxBeanOp = platformMXBeans.stream()
                    .filter(bufferPoolMXBean -> bufferPoolMXBean.getName().equals("direct")).findAny();
            getDirectMemoryUsage = () -> poolMxBeanOp.isPresent() ? poolMxBeanOp.get().getMemoryUsed() : Double.NaN;
        }
    }
}
