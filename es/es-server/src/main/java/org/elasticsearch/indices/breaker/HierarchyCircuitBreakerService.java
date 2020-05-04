/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.breaker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * CircuitBreakerService that attempts to redistribute space between breakers
 * if tripped
 */
public class HierarchyCircuitBreakerService extends CircuitBreakerService {

    private static final Logger LOGGER = LogManager.getLogger(HierarchyCircuitBreakerService.class);

    private static final String CHILD_LOGGER_PREFIX = "org.elasticsearch.indices.breaker.";

    private static final MemoryMXBean MEMORY_MX_BEAN = ManagementFactory.getMemoryMXBean();
    private static final double PARENT_BREAKER_ESCAPE_HATCH_PERCENTAGE = 0.30;

    private final ConcurrentMap<String, CircuitBreaker> breakers = new ConcurrentHashMap<>();

    public static final Setting<ByteSizeValue> TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING =
        Setting.memorySizeSetting("indices.breaker.total.limit", "95%", Property.Dynamic, Property.NodeScope);

    public static final Setting<ByteSizeValue> FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING =
        Setting.memorySizeSetting("indices.breaker.fielddata.limit", "60%", Property.Dynamic, Property.NodeScope);
    public static final Setting<Double> FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING =
        Setting.doubleSetting("indices.breaker.fielddata.overhead", 1.03d, 0.0d, Property.Dynamic, Property.NodeScope);
    public static final Setting<CircuitBreaker.Type> FIELDDATA_CIRCUIT_BREAKER_TYPE_SETTING =
        new Setting<>("indices.breaker.fielddata.type", "memory", CircuitBreaker.Type::parseValue, Property.NodeScope);

    public static final Setting<ByteSizeValue> REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING =
        Setting.memorySizeSetting("indices.breaker.request.limit", "60%", Property.Dynamic, Property.NodeScope);
    public static final Setting<Double> REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING =
        Setting.doubleSetting("indices.breaker.request.overhead", 1.0d, 0.0d, Property.Dynamic, Property.NodeScope);
    public static final Setting<CircuitBreaker.Type> REQUEST_CIRCUIT_BREAKER_TYPE_SETTING =
        new Setting<>("indices.breaker.request.type", "memory", CircuitBreaker.Type::parseValue, Property.NodeScope);

    public static final Setting<ByteSizeValue> ACCOUNTING_CIRCUIT_BREAKER_LIMIT_SETTING =
        Setting.memorySizeSetting("indices.breaker.accounting.limit", "100%", Property.Dynamic, Property.NodeScope);
    public static final Setting<Double> ACCOUNTING_CIRCUIT_BREAKER_OVERHEAD_SETTING =
        Setting.doubleSetting("indices.breaker.accounting.overhead", 1.0d, 0.0d, Property.Dynamic, Property.NodeScope);
    public static final Setting<CircuitBreaker.Type> ACCOUNTING_CIRCUIT_BREAKER_TYPE_SETTING =
        new Setting<>("indices.breaker.accounting.type", "memory", CircuitBreaker.Type::parseValue, Property.NodeScope);

    public static final Setting<ByteSizeValue> IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING =
        Setting.memorySizeSetting("network.breaker.inflight_requests.limit", "100%", Property.Dynamic, Property.NodeScope);
    public static final Setting<Double> IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING =
        Setting.doubleSetting("network.breaker.inflight_requests.overhead", 1.0d, 0.0d, Property.Dynamic, Property.NodeScope);
    public static final Setting<CircuitBreaker.Type> IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_TYPE_SETTING =
        new Setting<>("network.breaker.inflight_requests.type", "memory", CircuitBreaker.Type::parseValue, Property.NodeScope);

    private volatile BreakerSettings parentSettings;
    private volatile BreakerSettings fielddataSettings;
    private volatile BreakerSettings inFlightRequestsSettings;
    private volatile BreakerSettings requestSettings;
    private volatile BreakerSettings accountingSettings;

    // Tripped count for when redistribution was attempted but wasn't successful
    private final AtomicLong parentTripCount = new AtomicLong(0);

    public HierarchyCircuitBreakerService(Settings settings, ClusterSettings clusterSettings) {
        this.fielddataSettings = new BreakerSettings(CircuitBreaker.FIELDDATA,
                FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes(),
                FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING.get(settings),
                FIELDDATA_CIRCUIT_BREAKER_TYPE_SETTING.get(settings)
        );

        this.inFlightRequestsSettings = new BreakerSettings(CircuitBreaker.IN_FLIGHT_REQUESTS,
                IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes(),
                IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING.get(settings),
                IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_TYPE_SETTING.get(settings)
        );

        this.requestSettings = new BreakerSettings(CircuitBreaker.REQUEST,
                REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes(),
                REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.get(settings),
                REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.get(settings)
        );

        this.accountingSettings = new BreakerSettings(CircuitBreaker.ACCOUNTING,
                ACCOUNTING_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes(),
                ACCOUNTING_CIRCUIT_BREAKER_OVERHEAD_SETTING.get(settings),
                ACCOUNTING_CIRCUIT_BREAKER_TYPE_SETTING.get(settings)
        );

        this.parentSettings = new BreakerSettings(CircuitBreaker.PARENT,
                TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes(), 1.0,
                CircuitBreaker.Type.PARENT);

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("parent circuit breaker with settings {}", this.parentSettings);
        }

        registerBreaker(this.requestSettings);
        registerBreaker(this.fielddataSettings);
        registerBreaker(this.inFlightRequestsSettings);
        registerBreaker(this.accountingSettings);

        clusterSettings.addSettingsUpdateConsumer(TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING, this::setTotalCircuitBreakerLimit, this::validateTotalCircuitBreakerLimit);
        clusterSettings.addSettingsUpdateConsumer(FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING, FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING, this::setFieldDataBreakerLimit);
        clusterSettings.addSettingsUpdateConsumer(IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING, IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING, this::setInFlightRequestsBreakerLimit);
        clusterSettings.addSettingsUpdateConsumer(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING, REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING, this::setRequestBreakerLimit);
        clusterSettings.addSettingsUpdateConsumer(ACCOUNTING_CIRCUIT_BREAKER_LIMIT_SETTING, ACCOUNTING_CIRCUIT_BREAKER_OVERHEAD_SETTING, this::setAccountingBreakerLimit);
    }

    private void setRequestBreakerLimit(ByteSizeValue newRequestMax, Double newRequestOverhead) {
        BreakerSettings newRequestSettings = new BreakerSettings(CircuitBreaker.REQUEST, newRequestMax.getBytes(), newRequestOverhead,
                HierarchyCircuitBreakerService.this.requestSettings.getType());
        registerBreaker(newRequestSettings);
        HierarchyCircuitBreakerService.this.requestSettings = newRequestSettings;
        LOGGER.info("Updated breaker settings request: {}", newRequestSettings);
    }

    private void setInFlightRequestsBreakerLimit(ByteSizeValue newInFlightRequestsMax, Double newInFlightRequestsOverhead) {
        BreakerSettings newInFlightRequestsSettings = new BreakerSettings(CircuitBreaker.IN_FLIGHT_REQUESTS, newInFlightRequestsMax.getBytes(),
            newInFlightRequestsOverhead, HierarchyCircuitBreakerService.this.inFlightRequestsSettings.getType());
        registerBreaker(newInFlightRequestsSettings);
        HierarchyCircuitBreakerService.this.inFlightRequestsSettings = newInFlightRequestsSettings;
        LOGGER.info("Updated breaker settings for in-flight requests: {}", newInFlightRequestsSettings);
    }

    private void setFieldDataBreakerLimit(ByteSizeValue newFielddataMax, Double newFielddataOverhead) {
        long newFielddataLimitBytes = newFielddataMax == null ? HierarchyCircuitBreakerService.this.fielddataSettings.getLimit() : newFielddataMax.getBytes();
        newFielddataOverhead = newFielddataOverhead == null ? HierarchyCircuitBreakerService.this.fielddataSettings.getOverhead() : newFielddataOverhead;
        BreakerSettings newFielddataSettings = new BreakerSettings(CircuitBreaker.FIELDDATA, newFielddataLimitBytes, newFielddataOverhead,
                HierarchyCircuitBreakerService.this.fielddataSettings.getType());
        registerBreaker(newFielddataSettings);
        HierarchyCircuitBreakerService.this.fielddataSettings = newFielddataSettings;
        LOGGER.info("Updated breaker settings field data: {}", newFielddataSettings);
    }

    private void setAccountingBreakerLimit(ByteSizeValue newAccountingMax, Double newAccountingOverhead) {
        BreakerSettings newAccountingSettings = new BreakerSettings(CircuitBreaker.ACCOUNTING, newAccountingMax.getBytes(),
            newAccountingOverhead, HierarchyCircuitBreakerService.this.inFlightRequestsSettings.getType());
        registerBreaker(newAccountingSettings);
        HierarchyCircuitBreakerService.this.accountingSettings = newAccountingSettings;
        LOGGER.info("Updated breaker settings for accounting requests: {}", newAccountingSettings);
    }

    private boolean validateTotalCircuitBreakerLimit(ByteSizeValue byteSizeValue) {
        BreakerSettings newParentSettings = new BreakerSettings(CircuitBreaker.PARENT, byteSizeValue.getBytes(), 1.0, CircuitBreaker.Type.PARENT);
        validateSettings(new BreakerSettings[]{newParentSettings});
        return true;
    }

    private void setTotalCircuitBreakerLimit(ByteSizeValue byteSizeValue) {
        BreakerSettings newParentSettings = new BreakerSettings(CircuitBreaker.PARENT, byteSizeValue.getBytes(), 1.0, CircuitBreaker.Type.PARENT);
        this.parentSettings = newParentSettings;
    }

    /**
     * Validate that child settings are valid
     */
    public static void validateSettings(BreakerSettings[] childrenSettings) throws IllegalStateException {
        for (BreakerSettings childSettings : childrenSettings) {
            // If the child is disabled, ignore it
            if (childSettings.getLimit() == -1) {
                continue;
            }

            if (childSettings.getOverhead() < 0) {
                throw new IllegalStateException("Child breaker overhead " + childSettings + " must be non-negative");
            }
        }
    }

    @Override
    public CircuitBreaker getBreaker(String name) {
        return this.breakers.get(name);
    }

    @Override
    public CircuitBreakerStats stats(String name) {
        if (CircuitBreaker.PARENT.equals(name)) {
            return new CircuitBreakerStats(
                CircuitBreaker.PARENT,
                parentSettings.getLimit(),
                parentUsed(0L),
                parentTripCount.get(), 1.0
            );
        }
        CircuitBreaker breaker = requireNonNull(this.breakers.get(name), "Unknown circuit breaker: " + name);
        return new CircuitBreakerStats(
            breaker.getName(),
            breaker.getLimit(),
            breaker.getUsed(),
            breaker.getTrippedCount(),
            breaker.getOverhead());
    }

    private long parentUsed(long newBytesReserved) {
        return currentMemoryUsage() + newBytesReserved;
    }

    //package private to allow overriding it in tests
    long currentMemoryUsage() {
        try {
            return MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed();
        } catch (IllegalArgumentException ex) {
            // This exception can happen (rarely) due to a race condition in the JVM when determining usage of memory pools. We do not want
            // to fail requests because of this and thus return zero memory usage in this case. While we could also return the most
            // recently determined memory usage, we would overestimate memory usage immediately after a garbage collection event.
            assert ex.getMessage().matches("committed = \\d+ should be < max = \\d+");
            LOGGER.info("Cannot determine current memory usage due to JDK-8207200.", ex);
            return 0;
        }
    }

    /**
     * Checks whether the parent breaker has been tripped
     */
    public void checkParentLimit(long newBytesReserved, String label) throws CircuitBreakingException {
        long totalUsed = parentUsed(newBytesReserved);
        long parentLimit = this.parentSettings.getLimit();
        if (totalUsed > parentLimit) {
            long breakersTotalUsed = breakers.values().stream()
                .mapToLong(x -> (long) (x.getUsed() * x.getOverhead()))
                .sum();
            // if the individual breakers hardly use any memory we assume that there is a lot of heap usage by objects which can be GCd.
            // We want to allow the query so that it triggers GCs
            if ((breakersTotalUsed + newBytesReserved) < (parentLimit * PARENT_BREAKER_ESCAPE_HATCH_PERCENTAGE)) {
                return;
            }
            this.parentTripCount.incrementAndGet();
            final StringBuilder message = new StringBuilder(
                "[parent] Data too large, data for [" + label + "]" +
                " would be [" + totalUsed + "/" + new ByteSizeValue(totalUsed) + "]" +
                ", which is larger than the limit of [" +
                parentLimit + "/" + new ByteSizeValue(parentLimit) + "]");
            message.append(", usages [");
            message.append(this.breakers.entrySet().stream().map(e -> {
                final CircuitBreaker breaker = e.getValue();
                final long breakerUsed = (long)(breaker.getUsed() * breaker.getOverhead());
                return e.getKey() + "=" + breakerUsed + "/" + new ByteSizeValue(breakerUsed);
            }).collect(Collectors.joining(", ")));
            message.append("]");
            throw new CircuitBreakingException(message.toString(), totalUsed, parentLimit);
        }
    }

    /**
     * Allows to register a custom circuit breaker.
     * Warning: Will overwrite any existing custom breaker with the same name.
     */
    @Override
    public void registerBreaker(BreakerSettings breakerSettings) {
        // Validate the settings
        validateSettings(new BreakerSettings[] {breakerSettings});

        if (breakerSettings.getType() == CircuitBreaker.Type.NOOP) {
            CircuitBreaker breaker = new NoopCircuitBreaker(breakerSettings.getName());
            breakers.put(breakerSettings.getName(), breaker);
        } else {
            CircuitBreaker oldBreaker;
            CircuitBreaker breaker = new ChildMemoryCircuitBreaker(breakerSettings,
                    LogManager.getLogger(CHILD_LOGGER_PREFIX + breakerSettings.getName()),
                    this, breakerSettings.getName());

            for (;;) {
                oldBreaker = breakers.putIfAbsent(breakerSettings.getName(), breaker);
                if (oldBreaker == null) {
                    return;
                }
                breaker = new ChildMemoryCircuitBreaker(breakerSettings,
                        (ChildMemoryCircuitBreaker)oldBreaker,
                        LogManager.getLogger(CHILD_LOGGER_PREFIX + breakerSettings.getName()),
                        this, breakerSettings.getName());

                if (breakers.replace(breakerSettings.getName(), oldBreaker, breaker)) {
                    return;
                }
            }
        }

    }
}
