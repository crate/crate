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

import org.elasticsearch.common.unit.ByteSizeValue;

import java.beans.ConstructorProperties;

/**
 * Class encapsulating stats about the circuit breaker
 */
public class CircuitBreakerStats {

    private final String name;
    private final long limit;
    private final long used;
    private final long trippedCount;
    private final double overhead;

    @ConstructorProperties({"name", "limit", "used", "trippedCount", "overhead"})
    public CircuitBreakerStats(String name,
                               long limit,
                               long used,
                               long trippedCount,
                               double overhead) {
        this.name = name;
        this.limit = limit;
        this.used = used;
        this.trippedCount = trippedCount;
        this.overhead = overhead;
    }

    public String getName() {
        return this.name;
    }

    public long getLimit() {
        return this.limit;
    }

    public long getUsed() {
        return this.used;
    }

    public long getTrippedCount() {
        return this.trippedCount;
    }

    public double getOverhead() {
        return this.overhead;
    }

    @Override
    public String toString() {
        return "[" + this.name +
               ",limit=" + this.limit + "/" + new ByteSizeValue(this.limit) +
               ",estimated=" + this.used + "/" + new ByteSizeValue(this.used) +
               ",overhead=" + this.overhead + ",tripped=" + this.trippedCount + "]";
    }
}
