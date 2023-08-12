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
import java.util.List;
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


    private static final String METER_NAME = "org.apache.zookeeper";

    private static final List<Double> histogramBuckets = Arrays.asList(
            0.1, 0.2, 0.5,
            1.0, 2.0, 5.0,
            10.0, 20.0, 50.0,
            100.0, 200.0, 500.0,
            1_000.0, 2_000.0, 5_000.0,
            10_000.0, 20_000.0, 50_000.0
    );

    private final OpenTelemetry openTelemetry;

    final Meter meter;

    private boolean exportJvmInfo = true;

    public OtelMetricsProvider() {
        AutoConfiguredOpenTelemetrySdk sdk = AutoConfiguredOpenTelemetrySdk.builder()
                .addMeterProviderCustomizer(
                        (sdkMeterProviderBuilder, configProperties) ->
                                sdkMeterProviderBuilder.registerView(
                                        InstrumentSelector.builder()
                                                .setMeterName(METER_NAME)
                                                .setType(InstrumentType.HISTOGRAM)
                                                .build(),
                                        View.builder()
                                                .setAggregation(Aggregation.explicitBucketHistogram(histogramBuckets))
                                                .build())

                ).build();
        this.openTelemetry = sdk.getOpenTelemetrySdk();
        this.meter = openTelemetry.getMeter(METER_NAME);
    }


    @Override
    public void configure(Properties configuration) throws MetricsProviderLifeCycleException {
        this.exportJvmInfo = Boolean.parseBoolean(configuration.getProperty("exportJvmInfo", "true"));
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
    public void start() throws MetricsProviderLifeCycleException {

    }

    @Override
    public MetricsContext getRootContext() {
        return null;
    }

    @Override
    public void stop() {

    }

    @Override
    public void dump(BiConsumer<String, Object> sink) {

    }

    @Override
    public void resetAllValues() {

    }

    private class OtelContext implements MetricsContext {

        private final OtelMetricsProvider provider;
        private final String name;

        private final ConcurrentMap<String, OtelGaugeWrapper> gauges = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, OtelLabelledGaugeWrapper> gaugeSets = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, OtelCounter> counters = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, OtelCounterSet> counterSets = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, OtelSummary> summaries = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, OtelLableledSummary> summarySets = new ConcurrentHashMap<>();

        private OtelContext(OtelMetricsProvider provider, String name) {
            this.provider = provider;
            this.name = name;
        }

        @Override
        public MetricsContext getContext(String name) {
            return new OtelContext(provider, completeName(name));
        }

        private String completeName(String subName) {
            String metricName = name.isEmpty()
                    ? subName
                    : new StringJoiner(".").add(name).add(subName).toString();
            return metricName;
        }

        @Override
        public Counter getCounter(String name) {
            return counters.computeIfAbsent(completeName(name), k -> new OtelCounter(provider.meter, name));
        }


        @Override
        public CounterSet getCounterSet(final String name) {
            Objects.requireNonNull(name, "Cannot register a CounterSet with null name");
            return counterSets.computeIfAbsent(completeName(name), k -> new OtelCounterSet(provider.meter, name));
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
            OtelGaugeWrapper existing = gauges.remove(name);
            if (existing != null) {
                existing.unregister();
            }
        }

        @Override
        public void registerGaugeSet(final String name, final GaugeSet gaugeSet) {
            Objects.requireNonNull(name, "Cannot register a GaugeSet with null name");
            Objects.requireNonNull(gaugeSet, "Cannot register a null GaugeSet for " + name);

            gaugeSets.compute(name, (id, prev) ->
                    new OtelLabelledGaugeWrapper(meter, name, gaugeSet, prev != null ? prev.inner : null));
        }

        @Override
        public void unregisterGaugeSet(final String name) {
            Objects.requireNonNull(name, "Cannot unregister GaugeSet with null name");

            final OtelLabelledGaugeWrapper existing = gaugeSets.remove(name);
            if (existing != null) {
                existing.unregister();
            }
        }

        @Override
        public Summary getSummary(String name, DetailLevel detailLevel) {
            return summaries.compute(name, (k, v) -> {
                if (v != null) {
                    throw new IllegalArgumentException("Already registered a non basic summary as " + k);
                }
                return new OtelSummary(meter, name);
            });
        }

        @Override
        public SummarySet getSummarySet(String name, DetailLevel detailLevel) {
            return summarySets.computeIfAbsent(name, (n) -> {
                if (summarySets.containsKey(n)) {
                    throw new IllegalArgumentException("Already registered a basic summary set as " + n);
                }
                return new OtelLableledSummary(meter, name);
            });
        }
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

        public OtelSummary(Meter meter, String name) {
            histogram = meter.histogramBuilder(name).build();
        }

        @Override
        public void add(long value) {
            histogram.record(value);
        }
    }

    private class OtelLableledSummary implements SummarySet {

        private final DoubleHistogram histogram;

        public OtelLableledSummary(Meter meter, String name) {
            histogram = meter.histogramBuilder(name).build();
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
