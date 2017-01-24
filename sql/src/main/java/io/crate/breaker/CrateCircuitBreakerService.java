/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.breaker;

import com.google.common.collect.ArrayListMultimap;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.Locale;

import static org.elasticsearch.common.breaker.CircuitBreaker.Type;

public class CrateCircuitBreakerService extends CircuitBreakerService {

    public static final String QUERY = "query";
    public static final String QUERY_CIRCUIT_BREAKER_LIMIT_SETTING = "indices.breaker.query.limit";
    public static final String DEFAULT_QUERY_CIRCUIT_BREAKER_LIMIT= "60%";
    public static final String QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING = "indices.breaker.query.overhead";
    public static final double DEFAULT_QUERY_CIRCUIT_BREAKER_OVERHEAD_CONSTANT = 1.09;

    public static final String JOBS_LOG = "jobs_log";
    public static final String JOBS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING = "stats.breaker.log.jobs.limit";
    public static final String DEFAULT_JOBS_LOG_CIRCUIT_BREAKER_LIMIT = "5%";
    public static final String JOBS_LOG_CIRCUIT_BREAKER_OVERHEAD_SETTING = "stats.breaker.log.jobs.overhead";

    public static final String OPERATIONS_LOG = "operations_log";
    public static final String OPERATIONS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING = "stats.breaker.log.operations.limit";
    public static final String DEFAULT_OPERATIONS_LOG_CIRCUIT_BREAKER_LIMIT= "5%";
    public static final String OPERATIONS_LOG_CIRCUIT_BREAKER_OVERHEAD_SETTING = "stats.breaker.log.operations.overhead";

    public static final double DEFAULT_LOG_CIRCUIT_BREAKER_OVERHEAD_CONSTANT = 1.0;

    static final String BREAKING_EXCEPTION_MESSAGE =
        "[query] Data too large, data for [%s] would be larger than limit of [%d/%s]";

    private final CircuitBreakerService esCircuitBreakerService;

    private final ArrayListMultimap<String, Listener> listeners = ArrayListMultimap.create();

    @Inject
    public CrateCircuitBreakerService(Settings settings,
                                      NodeSettingsService nodeSettingsService,
                                      CircuitBreakerService esCircuitBreakerService) {
        super(settings);
        this.esCircuitBreakerService = esCircuitBreakerService;

        BreakerSettings breakerSettings;

        breakerSettings = new BreakerSettings(QUERY,
            settings.getAsMemory(
                QUERY_CIRCUIT_BREAKER_LIMIT_SETTING,
                DEFAULT_QUERY_CIRCUIT_BREAKER_LIMIT).getBytes(),
            settings.getAsDouble(
                QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING,
                DEFAULT_QUERY_CIRCUIT_BREAKER_OVERHEAD_CONSTANT),
            Type.MEMORY);
        registerBreaker(breakerSettings);

        breakerSettings = new BreakerSettings(JOBS_LOG,
            settings.getAsMemory(
                JOBS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING,
                DEFAULT_JOBS_LOG_CIRCUIT_BREAKER_LIMIT).getBytes(),
            settings.getAsDouble(
                JOBS_LOG_CIRCUIT_BREAKER_OVERHEAD_SETTING,
                DEFAULT_LOG_CIRCUIT_BREAKER_OVERHEAD_CONSTANT),
            Type.MEMORY);
        registerBreaker(breakerSettings);

        breakerSettings = new BreakerSettings(OPERATIONS_LOG,
            settings.getAsMemory(
                OPERATIONS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING,
                DEFAULT_OPERATIONS_LOG_CIRCUIT_BREAKER_LIMIT).getBytes(),
            settings.getAsDouble(
                OPERATIONS_LOG_CIRCUIT_BREAKER_OVERHEAD_SETTING,
                DEFAULT_LOG_CIRCUIT_BREAKER_OVERHEAD_CONSTANT),
            Type.MEMORY);
        registerBreaker(breakerSettings);

        nodeSettingsService.addListener(new ApplySettings());
    }

    @Override
    public void registerBreaker(BreakerSettings breakerSettings) {
        esCircuitBreakerService.registerBreaker(breakerSettings);
    }

    @Override
    public CircuitBreaker getBreaker(String name) {
        return esCircuitBreakerService.getBreaker(name);
    }

    @Override
    public AllCircuitBreakerStats stats() {
        return esCircuitBreakerService.stats();
    }

    @Override
    public CircuitBreakerStats stats(String name) {
        return esCircuitBreakerService.stats(name);
    }

    public static String breakingExceptionMessage(String label, long limit) {
        return String.format(Locale.ENGLISH, BREAKING_EXCEPTION_MESSAGE, label,
            limit, new ByteSizeValue(limit));
    }

    private class ApplySettings implements NodeSettingsService.Listener {

        @Override
        public void onRefreshSettings(Settings settings) {
            // Query breaker settings
            registerBreakerSettings(QUERY,
                settings,
                QUERY_CIRCUIT_BREAKER_LIMIT_SETTING,
                DEFAULT_QUERY_CIRCUIT_BREAKER_LIMIT,
                QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING,
                DEFAULT_QUERY_CIRCUIT_BREAKER_OVERHEAD_CONSTANT);

            // Jobs log breaker settings
            registerBreakerSettings(JOBS_LOG,
                settings,
                JOBS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING,
                DEFAULT_JOBS_LOG_CIRCUIT_BREAKER_LIMIT,
                JOBS_LOG_CIRCUIT_BREAKER_OVERHEAD_SETTING,
                DEFAULT_LOG_CIRCUIT_BREAKER_OVERHEAD_CONSTANT);

            // Operations log breaker settings
            registerBreakerSettings(OPERATIONS_LOG,
                settings,
                OPERATIONS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING,
                DEFAULT_OPERATIONS_LOG_CIRCUIT_BREAKER_LIMIT,
                OPERATIONS_LOG_CIRCUIT_BREAKER_OVERHEAD_SETTING,
                DEFAULT_LOG_CIRCUIT_BREAKER_OVERHEAD_CONSTANT);
        }

        private void registerBreakerSettings(String name,
                                             Settings newSettings,
                                             String limitSettingName,
                                             String defaultLimit,
                                             String overheadSettingName,
                                             double defaultOverhead) {
            CircuitBreaker currentBreaker = getBreaker(name);

            long initialLimit = CrateCircuitBreakerService.this.settings.getAsMemory(
                limitSettingName, defaultLimit
            ).getBytes();
            long limit = newSettings.getAsMemory(
                limitSettingName, String.format(Locale.ENGLISH, "%db", initialLimit) // we need to pass exact bytes value with `b` suffix in order not to get into rounding issues
            ).getBytes();
            double initialOverhead = CrateCircuitBreakerService.this.settings.getAsDouble(
                overheadSettingName, defaultOverhead
            );
            double overhead = newSettings.getAsDouble(
                overheadSettingName, initialOverhead
            );
            if (limit != currentBreaker.getLimit() || overhead != currentBreaker.getOverhead()) {
                BreakerSettings newBreakerSettings = new BreakerSettings(name, limit, overhead, Type.MEMORY);
                registerBreaker(newBreakerSettings);
                // inform listeners about the change
                for (Listener listener : listeners.get(name)) {
                    listener.onBreakerChanged(getBreaker(name));
                }
            }
        }
    }

    /**
     * Add listener to a certain breaker
     * in order to get informed whenever the breaker settings change.
     * This is not thread-safe.
     */
    public void addBreakerChangeListener(String name, CrateCircuitBreakerService.Listener listener) {
        listeners.put(name, listener);
    }

    public interface Listener {
        /**
         * Callback that is called whenever the settings of a breaker had changed.
         */
        void onBreakerChanged(CircuitBreaker breaker);
    }
}
