/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operator.operations.collect;

import io.crate.operator.Input;
import io.crate.operator.RowCollector;
import io.crate.operator.aggregation.CollectExpression;

import java.util.Set;

/**
 * Simple Collector that only collects one row and does not support any query or aggregation
 */
public class SimpleOneRowCollector implements RowCollector<Object[][]> {

    private final Input<?>[] inputs;
    private final Set<CollectExpression<?>> collectExpressions;
    private final Object[] result;

    public SimpleOneRowCollector(Input<?>[] inputs, Set<CollectExpression<?>> collectExpressions) {
        this.inputs = inputs;
        this.result = new Object[inputs.length];
        this.collectExpressions = collectExpressions;
    }

    @Override
    public boolean startCollect() {
        for (CollectExpression<?> collectExpression : collectExpressions) {
            collectExpression.startCollect();
        }
        return false;
    }

    @Override
    public boolean processRow() {
        return false;
    }

    @Override
    public Object[][] finishCollect() {
        int i = 0;
        if (inputs == null || inputs.length == 0) {
            return new Object[0][];
        } else {
            for (Input<?> input : inputs) {
                result[i++] = input.value();
            }
            return new Object[][]{ result };
        }
    }
}
