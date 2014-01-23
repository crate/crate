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

import java.util.Arrays;

/**
 * Simple Collector that only collects one row and does not support any query or aggregation
 */
public class SimpleCollector implements RowCollector<Object[][]> {

    private final Input<?>[] inputs;
    private final CollectExpression<?>[] outputs;
    private Object[] result = null;
    private Object[] inputResults;

    public SimpleCollector(Input<?>[] inputs, CollectExpression<?>[] outputs) {
        this.inputs = inputs;
        this.outputs = outputs;
        this.inputResults = new Object[this.inputs.length];
    }

    @Override
    public boolean startCollect() {
        for (int i=0; i<outputs.length; i++) {
            outputs[i].startCollect();
        }
        return outputs.length > 0;
    }

    @Override
    public boolean processRow() {
        result = new Object[outputs.length];

        Arrays.fill(inputResults, null);
        for (int i=0; i<inputs.length; i++) {
            inputResults[i] = inputs[i].value();
        }
        for (int i=0; i<outputs.length; i++) {
            outputs[i].setNextRow(inputResults);
            result[i] = outputs[i].value();
        }
        return false;
    }

    @Override
    public Object[][] finishCollect() {
        if (result == null) {
            return new Object[0][];
        } else {
            return new Object[][]{result};
        }
    }
}
