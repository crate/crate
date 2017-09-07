/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.aggregation;

import io.crate.data.BatchIterator;
import io.crate.data.Columns;
import io.crate.data.ForwardingBatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowBridging;
import io.crate.operation.collect.CollectExpression;

import java.util.List;

/**
 * BatchIterator implementation that can transform rows using {@link Input}s and {@link CollectExpression}s.
 *
 * Example:
 * <pre>
 *     BatchIterator source data:
 *      [ 1, 2, 3 ]
 *
 *     Input: add(inputColumn(0), 2)
 *     expressions: InputColumnExpression
 *
 *     output:
 *      [ 3, 4, 5 ]
 * </pre>
 *
 * This is similar to the `map` function (from the stream API), except that the transformation happens using a
 * shared object via stateful inputs/expressions.
 */
public class RowTransformingBatchIterator extends ForwardingBatchIterator {

    private final BatchIterator delegate;
    private final Iterable<? extends CollectExpression<Row, ?>> expressions;
    private final Row sourceRow;
    private final Columns rowData;

    public RowTransformingBatchIterator(BatchIterator delegate,
                                        List<? extends Input<?>> inputs,
                                        Iterable<? extends CollectExpression<Row, ?>> expressions) {
        this.delegate = delegate;
        this.expressions = expressions;
        this.sourceRow = RowBridging.toRow(delegate.rowData());
        this.rowData = Columns.wrap(inputs);
    }

    @Override
    public Columns rowData() {
        return rowData;
    }

    @Override
    protected BatchIterator delegate() {
        return delegate;
    }

    @Override
    public boolean moveNext() {
        if (delegate.moveNext()) {
            for (CollectExpression<Row, ?> expression : expressions) {
                expression.setNextRow(sourceRow);
            }
            return true;
        }
        return false;
    }
}
