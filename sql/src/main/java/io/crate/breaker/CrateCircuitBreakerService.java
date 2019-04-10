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

import io.crate.settings.CrateSetting;
import io.crate.types.DataTypes;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
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
import java.util.function.Consumer;

@Singleton
public class CrateCircuitBreakerService extends CircuitBreakerService {

    public static final String QUERY = "query";

    public static final CrateSetting<ByteSizeValue> QUERY_CIRCUIT_BREAKER_LIMIT_SETTING = CrateSetting.of(Setting.memorySizeSetting(
        "indices.breaker.query.limit", "60%", Setting.Property.Dynamic, Setting.Property.NodeScope), DataTypes.STRING);
    public static final CrateSetting<Double> QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING = CrateSetting.of(Setting.doubleSetting(
        "indices.breaker.query.overhead", 1.09d, 0.0d, Setting.Property.Dynamic, Setting.Property.NodeScope),DataTypes.DOUBLE);

    public static final String JOBS_LOG = "jobs_log";
    public static final CrateSetting<ByteSizeValue> JOBS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING = CrateSetting.of(Setting.memorySizeSetting(
        "stats.breaker.log.jobs.limit", "5%", Setting.Property.Dynamic, Setting.Property.NodeScope), DataTypes.STRING);
    public static final CrateSetting<Double> JOBS_LOG_CIRCUIT_BREAKER_OVERHEAD_SETTING = CrateSetting.of(Setting.doubleSetting(
        "stats.breaker.log.jobs.overhead", 1.0d, 0.0d, Setting.Property.Dynamic, Setting.Property.NodeScope), DataTypes.DOUBLE);

    public static final String OPERATIONS_LOG = "operations_log";
    public static final CrateSetting<ByteSizeValue> OPERATIONS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING = CrateSetting.of(Setting.memorySizeSetting(
        "stats.breaker.log.operations.limit", "5%", Setting.Property.Dynamic, Setting.Property.NodeScope), DataTypes.STRING);
    public static final CrateSetting<Double> OPERATIONS_LOG_CIRCUIT_BREAKER_OVERHEAD_SETTING = CrateSetting.of(Setting.doubleSetting(
        "stats.breaker.log.operations.overhead", 1.0d, 0.0d, Setting.Property.Dynamic, Setting.Property.NodeScope), DataTypes.DOUBLE);

    static final String BREAKING_EXCEPTION_MESSAGE =
        "[query] Data too large, data for [%s] would be larger than limit of [%d/%s]";

    private final CircuitBreakerService esCircuitBreakerService;
    private volatile BreakerSettings queryBreakerSettings;
    private volatile BreakerSettings logJobsBreakerSettings;
    private volatile BreakerSettings logOperationsBreakerSettings;

    @Inject
    public CrateCircuitBreakerService(Settings settings,
                                      ClusterSettings clusterSettings,
                                      CircuitBreakerService esCircuitBreakerService) {
        super(settings);
        this.esCircuitBreakerService = esCircuitBreakerService;

        queryBreakerSettings = new BreakerSettings(QUERY,
            QUERY_CIRCUIT_BREAKER_LIMIT_SETTING.setting().get(settings).getBytes(),
            QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING.setting().get(settings),
            CircuitBreaker.Type.MEMORY
        );

        logJobsBreakerSettings = new BreakerSettings(JOBS_LOG,
            JOBS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING.setting().get(settings).getBytes(),
            JOBS_LOG_CIRCUIT_BREAKER_OVERHEAD_SETTING.setting().get(settings),
            CircuitBreaker.Type.MEMORY);

        logOperationsBreakerSettings = new BreakerSettings(OPERATIONS_LOG,
            OPERATIONS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING.setting().get(settings).getBytes(),
            OPERATIONS_LOG_CIRCUIT_BREAKER_OVERHEAD_SETTING.setting().get(settings),
            CircuitBreaker.Type.MEMORY);

        registerBreaker(queryBreakerSettings);
        registerBreaker(logJobsBreakerSettings);
        registerBreaker(logOperationsBreakerSettings);

        clusterSettings.addSettingsUpdateConsumer(QUERY_CIRCUIT_BREAKER_LIMIT_SETTING.setting(), QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING.setting(),
            (newLimit, newOverhead) ->
                setQueryBreakerLimit(queryBreakerSettings, QUERY, s -> this.queryBreakerSettings = s, newLimit, newOverhead));
        clusterSettings.addSettingsUpdateConsumer(JOBS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING.setting(),
            (newLimit) ->
                setQueryBreakerLimit(logJobsBreakerSettings, JOBS_LOG, s -> this.logJobsBreakerSettings = s, newLimit, null));
        clusterSettings.addSettingsUpdateConsumer(OPERATIONS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING.setting(),
            (newLimit) ->
                setQueryBreakerLimit(logOperationsBreakerSettings, OPERATIONS_LOG, s -> this.logOperationsBreakerSettings = s, newLimit, null));
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

    @Override
    public void checkParentLimit(String label) throws CircuitBreakingException {
        esCircuitBreakerService.checkParentLimit(label);
    }

    public static String breakingExceptionMessage(String label, long limit) {
        return String.format(Locale.ENGLISH, BREAKING_EXCEPTION_MESSAGE, label,
            limit, new ByteSizeValue(limit));
    }

    private void setQueryBreakerLimit(BreakerSettings oldSettings,
                                      String breakerName,
                                      Consumer<BreakerSettings> settingsConsumer,
                                      ByteSizeValue newLimit, Double newOverhead) {
        long newLimitBytes = newLimit == null ? oldSettings.getLimit() : newLimit.getBytes();
        newOverhead = newOverhead == null ? oldSettings.getOverhead() : newOverhead;
        BreakerSettings newSettings = new BreakerSettings(breakerName, newLimitBytes, newOverhead, oldSettings.getType());
        registerBreaker(newSettings);
        settingsConsumer.accept(newSettings);
        logger.info("[{}] Updated breaker settings: {}", breakerName, newSettings);
    }
}
