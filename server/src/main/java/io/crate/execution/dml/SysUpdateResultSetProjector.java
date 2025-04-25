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

package io.crate.execution.dml;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collector;

import io.crate.data.BatchIterator;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.CollectionBucket;
import io.crate.data.Input;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.reference.sys.SysRowUpdater;
import io.crate.expression.reference.sys.check.node.SysNodeCheck;

public class SysUpdateResultSetProjector implements Projector {

    private final Consumer<Object> rowWriter;
    private final List<NestableCollectExpression<SysNodeCheck, ?>> expressions;
    private final List<Input<?>> inputs;
    private final SysRowUpdater<?> sysRowUpdater;

    public SysUpdateResultSetProjector(SysRowUpdater<?> sysRowUpdater,
                                       Consumer<Object> rowWriter,
                                       List<NestableCollectExpression<SysNodeCheck, ?>> expressions,
                                       List<Input<?>> inputs) {
        this.sysRowUpdater = sysRowUpdater;
        this.rowWriter = rowWriter;
        this.expressions = expressions;
        this.inputs = inputs;
    }

    @Override
    public BatchIterator<Row> apply(BatchIterator<Row> batchIterator) {
        return CollectingBatchIterator
            .newInstance(batchIterator,
                         Collector.of(
                             ArrayList<Object[]>::new,
                             (acc, row) -> {
                                 Object[] returnValues = evaluateReturnValues(row);
                                 acc.add(returnValues);
                             },
                             (_, _) -> {
                                 throw new UnsupportedOperationException(
                                     "Combine not supported");
                             },
                             CollectionBucket::new
                             ));
    }

    private Object[] evaluateReturnValues(Row row) {
        Object sysNodeCheckId = row.get(0);
        //Update sysNodeCheck to the new value
        rowWriter.accept(sysNodeCheckId);
        //Retrieve updated sysNodeCheck and evaluate return values
        SysNodeCheck sysNodeCheck = (SysNodeCheck) sysRowUpdater.getRow(sysNodeCheckId);
        expressions.forEach(x -> x.setNextRow(sysNodeCheck));
        Object[] result = new Object[inputs.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = inputs.get(i).value();
        }
        return result;
    }

    @Override
    public boolean providesIndependentScroll() {
        return true;
    }

}
