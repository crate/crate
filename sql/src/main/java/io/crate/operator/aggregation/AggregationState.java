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

package io.crate.operator.aggregation;

import org.elasticsearch.common.io.stream.Streamable;

import java.util.Set;

/**
 * State of a aggregation function
 *
 * Note on serialization:
 *      In order to read the correct concrete AggState class on the receiver
 *      the receiver has to get the ParsedStatement beforehand and then use it
 *      to instantiate the correct concrete AggState instances.
 */
public abstract class AggregationState<T extends AggregationState> implements Comparable<T>, Streamable {

    public abstract Object value();
    public abstract void reduce(T other);

    /**
     * called after the rows/state have been merged on the reducer,
     * but before the rows are sent to the handler.
     */
    public void terminatePartial() {
        // noop;
    }


    /**
     * can be used to get a reference to a set of unique values
     * the set is managed by the aggStates encapsulating GroupByRow
     */
    public void setSeenValuesRef(Set<Object> seenValues) {
    }
}
