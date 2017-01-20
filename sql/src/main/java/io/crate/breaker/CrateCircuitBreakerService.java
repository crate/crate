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

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;

import java.util.Locale;

@Singleton
public class CrateCircuitBreakerService extends CircuitBreakerService {

    public static final String QUERY = "query";
    /*
    public static final String QUERY_CIRCUIT_BREAKER_LIMIT_SETTING = "indices.breaker.query.limit";
    public static final String DEFAULT_QUERY_CIRCUIT_BREAKER_LIMIT= "60%";
    public static final String QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING = "indices.breaker.query.overhead";
    public static final double DEFAULT_QUERY_CIRCUIT_BREAKER_OVERHEAD_CONSTANT = 1.09;
    */

    public static final Setting<ByteSizeValue> QUERY_CIRCUIT_BREAKER_LIMIT_SETTING =
        Setting.memorySizeSetting("indices.breaker.query.limit", "60%", Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final Setting<Double> QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING =
        Setting.doubleSetting("indices.breaker.query.overhead", 1.09d, 0.0d, Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final Setting<CircuitBreaker.Type> QUERY_CIRCUIT_BREAKER_TYPE_SETTING =
        new Setting<>("indices.breaker.query.type", "memory", CircuitBreaker.Type::parseValue, Setting.Property.NodeScope);

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

    //private final ArrayListMultimap<String, Listener> listeners = ArrayListMultimap.create();

    @Inject
    public CrateCircuitBreakerService(Settings settings,
                                      ClusterSettings clusterSettings,
                                      CircuitBreakerService esCircuitBreakerService) {
        super(settings);
        this.esCircuitBreakerService = esCircuitBreakerService;

        /*
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

        queryBreakerSettings = new BreakerSettings(QUERY,
            QUERY_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes(),
            QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING.get(settings),
            QUERY_CIRCUIT_BREAKER_TYPE_SETTING.get(settings)
        );

        registerBreaker(queryBreakerSettings);
        */
        clusterSettings.addSettingsUpdateConsumer(QUERY_CIRCUIT_BREAKER_LIMIT_SETTING, QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING, this::setQueryBreakerLimit);
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

    private void setQueryBreakerLimit(ByteSizeValue newQueryMax, Double newQueryOverhead) {
        /*
        long newQueryLimitBytes = newQueryMax == null ? CrateCircuitBreakerService.this.queryBreakerSettings.getLimit() : newQueryMax.getBytes();
        newQueryOverhead = newQueryOverhead == null ? CrateCircuitBreakerService.this.queryBreakerSettings.getOverhead() : newQueryOverhead;
        BreakerSettings newQuerySettings = new BreakerSettings(QUERY, newQueryLimitBytes, newQueryOverhead,
            CrateCircuitBreakerService.this.queryBreakerSettings.getType());
        registerBreaker(newQuerySettings);
        CrateCircuitBreakerService.this.queryBreakerSettings = newQuerySettings;
        logger.info("Updated breaker settings query: {}", newQuerySettings);
        */
    }
}
