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

package io.crate.operator.collector;

import com.google.common.base.Preconditions;
import io.crate.operator.Input;
import io.crate.operator.RowCollector;
import io.crate.operator.aggregation.AggregationCollector;
import io.crate.operator.aggregation.AggregationFunction;
import io.crate.operator.aggregation.AggregationState;
import io.crate.planner.symbol.Aggregation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroupingCollector implements RowCollector<Object[][]> {

    private final Input[] groupKeys;
    private final Aggregation[] aggregations;

    private final Map<List<Object>, AggregationState[]> result;
    private final AggregationCollector[] aggregationCollectors;

    public GroupingCollector(Input[] groupKeys,
                             Input[] aggregationInput,
                             Aggregation[] aggregations,
                             AggregationFunction[] functions) {
        Preconditions.checkArgument(aggregationInput.length == aggregations.length);
        Preconditions.checkArgument(aggregations.length == functions.length);

        this.groupKeys = groupKeys;
        this.aggregations = aggregations;

        aggregationCollectors = new AggregationCollector[aggregations.length];
        for (int i = 0; i < aggregations.length; i++) {
            // TODO: check Steps in aggregation

            aggregationCollectors[i] = new AggregationCollector(
                    aggregations[i],
                    functions[i],
                    aggregationInput[i]
            );
        }

        result = new HashMap<>();
    }

    @Override
    public boolean startCollect() {
        return true;
    }

    @Override
    public boolean processRow() {

        // TODO: use something with better equals() performance for the keys
        List<Object> key = new ArrayList<>(groupKeys.length);
        for (Input groupKey : groupKeys) {
            key.add(groupKey.value());
        }

        AggregationState[] states = result.get(key);
        if (states == null) {
            states = new AggregationState[aggregations.length];
            for (int i = 0; i < aggregationCollectors.length; i++) {
                aggregationCollectors[i].startCollect();
                aggregationCollectors[i].processRow();
                states[i] = aggregationCollectors[i].state();
            }
            result.put(key, states);
        } else {
            for (AggregationCollector aggregationCollector : aggregationCollectors) {
                aggregationCollector.processRow();
            }
        }

        return true;
    }

    @Override
    public Object[][] finishCollect() {
        Object[][] rows = new Object[result.size()][groupKeys.length + aggregations.length];

        int r = 0;
        for (Map.Entry<List<Object>, AggregationState[]> entry : result.entrySet()) {
            int c = 0;

            for (Object o : entry.getKey()) {
                rows[r][c] = o;
                c++;
            }
            for (AggregationState aggregationState : entry.getValue()) {
                rows[r][c] = aggregationState.value();
                c++;
            }
            r++;
        }

        return rows;
    }
}
