/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.beans;

import io.crate.breaker.CrateCircuitBreakerService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;

import java.beans.ConstructorProperties;

public class CircuitBreakers implements CircuitBreakersMXBean {

    public static final String NAME = "io.crate.monitoring:type=CircuitBreakers";

    public static class Stats {

        private final String name;
        private final long limit;
        private final long used;
        private final long trippedCount;
        private final double overhead;

        @SuppressWarnings("WeakerAccess")
        @ConstructorProperties({"name", "limit", "used", "trippedCount", "overhead"})
        public Stats(String name, long limit, long used, long trippedCount, double overhead) {
            this.name = name;
            this.limit = limit;
            this.used = used;
            this.trippedCount = trippedCount;
            this.overhead = overhead;
        }

        @SuppressWarnings("unused")
        public String getName() {
            return name;
        }

        @SuppressWarnings("unused")
        public long getLimit() {
            return limit;
        }

        @SuppressWarnings("unused")
        public long getUsed() {
            return used;
        }

        @SuppressWarnings("unused")
        public long getTrippedCount() {
            return trippedCount;
        }

        @SuppressWarnings("unused")
        public double getOverhead() {
            return overhead;
        }
    }

    private final CircuitBreakerService circuitBreakerService;

    public CircuitBreakers(CircuitBreakerService circuitBreakerService) {
        this.circuitBreakerService = circuitBreakerService;
    }

    private Stats getStats(String name) {
        CircuitBreakerStats stats = circuitBreakerService.stats(name);
        return new Stats(
            stats.getName(), stats.getLimit(), stats.getEstimated(), stats.getTrippedCount(), stats.getOverhead());
    }

    @Override
    public Stats getParent() {
        CircuitBreakerStats stats = circuitBreakerService.stats().getStats(CircuitBreaker.PARENT);
        return new Stats(
            stats.getName(), stats.getLimit(), stats.getEstimated(), stats.getTrippedCount(), stats.getOverhead());
    }

    @Override
    public Stats getFieldData() {
        return getStats(CircuitBreaker.FIELDDATA);
    }

    @Override
    public Stats getInFlightRequests() {
        return getStats(CircuitBreaker.IN_FLIGHT_REQUESTS);
    }

    @Override
    public Stats getRequest() {
        return getStats(CircuitBreaker.REQUEST);
    }

    @Override
    public Stats getQuery() {
        return getStats(CrateCircuitBreakerService.QUERY);
    }

    @Override
    public Stats getJobsLog() {
        return getStats(CrateCircuitBreakerService.JOBS_LOG);
    }

    @Override
    public Stats getOperationsLog() {
        return getStats(CrateCircuitBreakerService.OPERATIONS_LOG);
    }
}
