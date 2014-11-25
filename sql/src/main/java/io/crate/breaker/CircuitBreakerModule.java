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
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;

public class CircuitBreakerModule extends AbstractModule {

    public static final String QUERY_CIRCUIT_BREAKER_LIMIT_SETTING = "crate.breaker.query.limit";
    public static final String QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING = "crate.breaker.query.overhead";
    public static final String DEFAULT_CIRCUIT_BREAKER_LIMIT = "60%";
    public static final double DEFAULT_CIRCUIT_BREAKER_OVERHEAD_CONSTANT = 1.03;


    private final Settings settings;

    public CircuitBreakerModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        ByteSizeValue memoryLimit = settings.getAsMemory(
                QUERY_CIRCUIT_BREAKER_LIMIT_SETTING,
                DEFAULT_CIRCUIT_BREAKER_LIMIT);
        double overhead = settings.getAsDouble(
                QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING,
                DEFAULT_CIRCUIT_BREAKER_OVERHEAD_CONSTANT);

        ESLogger logger = Loggers.getLogger(QueryOperationCircuitBreaker.class);

        bind(CircuitBreaker.class).annotatedWith(QueryOperationCircuitBreaker.class)
                .toInstance(new MemoryCircuitBreaker(memoryLimit, overhead, logger));
    }
}
