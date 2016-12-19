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

import java.util.Locale;

public class CrateCircuitBreakerService extends CircuitBreakerService {

    public static final String QUERY_CIRCUIT_BREAKER_LIMIT_SETTING = "indices.breaker.query.limit";
    public static final String QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING = "indices.breaker.query.overhead";
    public static final String QUERY_CIRCUIT_BREAKER_TYPE_SETTING = "indices.breaker.query.type";
    public static final String DEFAULT_QUERY_CIRCUIT_BREAKER_LIMIT = "60%";
    public static final double DEFAULT_QUERY_CIRCUIT_BREAKER_OVERHEAD_CONSTANT = 1.09;
    public static final String DEFAULT_QUERY_CIRCUIT_BREAKER_TYPE = "memory";

    public static final String QUERY = "query";

    public static final String BREAKING_EXCEPTION_MESSAGE =
        "[query] Data too large, data for [%s] would be larger than limit of [%d/%s]";

    private final CircuitBreakerService esCircuitBreakerService;
    private BreakerSettings queryBreakerSettings;

    @Inject
    public CrateCircuitBreakerService(Settings settings,
                                      CircuitBreakerService esCircuitBreakerService) {
        super(settings);
        this.esCircuitBreakerService = esCircuitBreakerService;

        long memoryLimit = settings.getAsMemory(
            QUERY_CIRCUIT_BREAKER_LIMIT_SETTING,
            DEFAULT_QUERY_CIRCUIT_BREAKER_LIMIT).getBytes();
        double overhead = settings.getAsDouble(
            QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING,
            DEFAULT_QUERY_CIRCUIT_BREAKER_OVERHEAD_CONSTANT);

        queryBreakerSettings = new BreakerSettings(QUERY, memoryLimit, overhead,
            CircuitBreaker.Type.parseValue(
                settings.get(QUERY_CIRCUIT_BREAKER_TYPE_SETTING,
                    DEFAULT_QUERY_CIRCUIT_BREAKER_TYPE)));

        registerBreaker(queryBreakerSettings);
        //nodeSettingsService.addListener(new ApplySettings());
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
}
