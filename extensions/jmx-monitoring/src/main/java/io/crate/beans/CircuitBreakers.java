/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.beans;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;

public class CircuitBreakers implements CircuitBreakersMXBean {

    public static final String NAME = "io.crate.monitoring:type=CircuitBreakers";

    private final CircuitBreakerService circuitBreakerService;
    private final CircuitBreakerStats EMPTY_FIELDDATA_STATS = new CircuitBreakerStats("fielddata", -1, -1, 0, 0);

    public CircuitBreakers(CircuitBreakerService circuitBreakerService) {
        this.circuitBreakerService = circuitBreakerService;
    }

    @Override
    public CircuitBreakerStats getParent() {
        return circuitBreakerService.stats(CircuitBreaker.PARENT);
    }

    @Override
    public CircuitBreakerStats getFieldData() {
        return EMPTY_FIELDDATA_STATS;
    }

    @Override
    public CircuitBreakerStats getInFlightRequests() {
        return circuitBreakerService.stats(CircuitBreaker.IN_FLIGHT_REQUESTS);
    }

    @Override
    public CircuitBreakerStats getRequest() {
        return circuitBreakerService.stats(CircuitBreaker.REQUEST);
    }

    @Override
    public CircuitBreakerStats getQuery() {
        return circuitBreakerService.stats(CircuitBreaker.QUERY);
    }

    @Override
    public CircuitBreakerStats getJobsLog() {
        return circuitBreakerService.stats(CircuitBreaker.JOBS_LOG);
    }

    @Override
    public CircuitBreakerStats getOperationsLog() {
        return circuitBreakerService.stats(CircuitBreaker.OPERATIONS_LOG);
    }
}
