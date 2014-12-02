/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.aggregation;

import io.crate.breaker.RamAccountingContext;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.ByteSizeValue;

/**
 * State of a aggregation function
 *
 * Note on serialization:
 *      In order to read the correct concrete AggState class on the receiver
 *      the receiver has to get the ParsedStatement beforehand and then use it
 *      to instantiate the correct concrete AggState instances.
 */
public abstract class AggregationState<T extends AggregationState> implements Comparable<T>, Streamable {

    protected final RamAccountingContext ramAccountingContext;
    private final ESLogger logger = ESLoggerFactory.getLogger(getClass().getName());

    public AggregationState(RamAccountingContext ramAccountingContext) {
        this.ramAccountingContext = ramAccountingContext;
        // plain object size
        addEstimatedSize(8);
    }

    public abstract Object value();
    public abstract void reduce(T other) throws CircuitBreakingException;

    /**
     * called after the rows/state have been merged on the reducer,
     * but before the rows are sent to the handler.
     */
    public void terminatePartial() {
        // noop;
    }

    protected void addEstimatedSize(long size) throws CircuitBreakingException {
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] Adding {} bytes to RAM accounting context", getClass(), new ByteSizeValue(size));
        }
        ramAccountingContext.addBytes(size);
    }
}
