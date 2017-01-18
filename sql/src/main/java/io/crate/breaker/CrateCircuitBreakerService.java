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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.Locale;

public class CrateCircuitBreakerService extends CircuitBreakerService {

    public static final String QUERY = "query";
    public static final String QUERY_CIRCUIT_BREAKER_LIMIT_SETTING = "indices.breaker.query.limit";
    public static final String DEFAULT_QUERY_CIRCUIT_BREAKER_LIMIT= "60%";
    public static final String QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING = "indices.breaker.query.overhead";
    public static final double DEFAULT_QUERY_CIRCUIT_BREAKER_OVERHEAD_CONSTANT = 1.09;

    public static final String JOBS_LOG = "jobs_log";
    public static final String JOBS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING = "stats.breaker.jobs_log.limit";
    public static final String DEFAULT_JOBS_LOG_CIRCUIT_BREAKER_LIMIT = "5%";
    public static final String JOBS_LOG_CIRCUIT_BREAKER_OVERHEAD_SETTING = "stats.breaker.jobs_log.overhead";

    public static final String OPERATIONS_LOG = "operations_log";
    public static final String OPERATIONS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING = "stats.breaker.operations_log.limit";
    public static final String DEFAULT_OPERATIONS_LOG_CIRCUIT_BREAKER_LIMIT= "5%";
    public static final String OPERATIONS_LOG_CIRCUIT_BREAKER_OVERHEAD_SETTING = "stats.breaker.operations_log.overhead";

    public static final double DEFAULT_LOG_CIRCUIT_BREAKER_OVERHEAD_CONSTANT = 1.0;
    private static final String DEFAULT_CIRCUIT_BREAKER_TYPE = "memory";

    static final String BREAKING_EXCEPTION_MESSAGE =
        "[query] Data too large, data for [%s] would be larger than limit of [%d/%s]";

    private final CircuitBreakerService esCircuitBreakerService;
    private BreakerSettings queryBreakerSettings;
    private BreakerSettings jobsLogBreakerSettings;
    private BreakerSettings operationsLogBreakerSettings;

    @Inject
    public CrateCircuitBreakerService(Settings settings,
                                      NodeSettingsService nodeSettingsService,
                                      CircuitBreakerService esCircuitBreakerService) {
        super(settings);
        this.esCircuitBreakerService = esCircuitBreakerService;

        queryBreakerSettings = new BreakerSettings(QUERY,
            settings.getAsMemory(
                QUERY_CIRCUIT_BREAKER_LIMIT_SETTING,
                DEFAULT_QUERY_CIRCUIT_BREAKER_LIMIT).getBytes(),
            settings.getAsDouble(
                QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING,
                DEFAULT_QUERY_CIRCUIT_BREAKER_OVERHEAD_CONSTANT),
            CircuitBreaker.Type.parseValue(DEFAULT_CIRCUIT_BREAKER_TYPE));
        registerBreaker(queryBreakerSettings);

        jobsLogBreakerSettings = new BreakerSettings(JOBS_LOG,
            settings.getAsMemory(
                JOBS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING,
                DEFAULT_JOBS_LOG_CIRCUIT_BREAKER_LIMIT).getBytes(),
            settings.getAsDouble(
                JOBS_LOG_CIRCUIT_BREAKER_OVERHEAD_SETTING,
                DEFAULT_LOG_CIRCUIT_BREAKER_OVERHEAD_CONSTANT),
            CircuitBreaker.Type.parseValue(DEFAULT_CIRCUIT_BREAKER_TYPE));
        registerBreaker(jobsLogBreakerSettings);

        operationsLogBreakerSettings = new BreakerSettings(OPERATIONS_LOG,
            settings.getAsMemory(
                OPERATIONS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING,
                DEFAULT_OPERATIONS_LOG_CIRCUIT_BREAKER_LIMIT).getBytes(),
            settings.getAsDouble(
                OPERATIONS_LOG_CIRCUIT_BREAKER_OVERHEAD_SETTING,
                DEFAULT_LOG_CIRCUIT_BREAKER_OVERHEAD_CONSTANT),
            CircuitBreaker.Type.parseValue(DEFAULT_CIRCUIT_BREAKER_TYPE));
        registerBreaker(operationsLogBreakerSettings);

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

    public class ApplySettings implements NodeSettingsService.Listener {

        @Override
        public void onRefreshSettings(Settings settings) {
            // Query breaker settings
            registerBreakerSettings(QUERY,
                settings,
                QUERY_CIRCUIT_BREAKER_LIMIT_SETTING,
                DEFAULT_QUERY_CIRCUIT_BREAKER_LIMIT,
                QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING,
                DEFAULT_QUERY_CIRCUIT_BREAKER_OVERHEAD_CONSTANT,
                CrateCircuitBreakerService.this.queryBreakerSettings);

            // Jobs log breaker settings
            registerBreakerSettings(JOBS_LOG,
                settings,
                JOBS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING,
                DEFAULT_JOBS_LOG_CIRCUIT_BREAKER_LIMIT,
                JOBS_LOG_CIRCUIT_BREAKER_OVERHEAD_SETTING,
                DEFAULT_LOG_CIRCUIT_BREAKER_OVERHEAD_CONSTANT,
                CrateCircuitBreakerService.this.jobsLogBreakerSettings);

            // Operations log breaker settings
            registerBreakerSettings(OPERATIONS_LOG,
                settings,
                OPERATIONS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING,
                DEFAULT_OPERATIONS_LOG_CIRCUIT_BREAKER_LIMIT,
                OPERATIONS_LOG_CIRCUIT_BREAKER_OVERHEAD_SETTING,
                DEFAULT_LOG_CIRCUIT_BREAKER_OVERHEAD_CONSTANT,
                CrateCircuitBreakerService.this.operationsLogBreakerSettings);
        }

        private void registerBreakerSettings(String name,
                                             Settings newSettings,
                                             String limitSettingName,
                                             String defaultLimit,
                                             String overheadSettingName,
                                             double defaultOverhead,
                                             BreakerSettings oldBreakerSettings) {
            long limit = newSettings.getAsMemory(
                limitSettingName,
                CrateCircuitBreakerService.this.settings.getAsMemory(
                    limitSettingName,
                    defaultLimit
                ).toString()).getBytes();
            double overhead = newSettings.getAsDouble(
                overheadSettingName,
                CrateCircuitBreakerService.this.settings.getAsDouble(
                    overheadSettingName,
                    defaultOverhead
                ));
            if (limit != oldBreakerSettings.getLimit() || overhead != oldBreakerSettings.getOverhead()) {
                BreakerSettings breakerSettings = new BreakerSettings(
                    name, limit, overhead,
                    oldBreakerSettings.getType());
                registerBreaker(breakerSettings);
            }
        }
    }

}
