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
import io.crate.data.ForwardingBatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.operation.InputRow;
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
    private final InputRow inputRow;

    private Row currentRow;
    private boolean closed = false;

    public RowTransformingBatchIterator(BatchIterator delegate,
                                        List<? extends Input<?>> inputs,
                                        Iterable<? extends CollectExpression<Row, ?>> expressions) {
        this.delegate = delegate;
        this.expressions = expressions;
        this.inputRow = new InputRow(inputs);
        this.currentRow = inputRow;
    }

    @Override
    protected BatchIterator delegate() {
        return delegate;
    }

    @Override
    public boolean moveNext() {
        if (delegate.moveNext()) {
            for (CollectExpression<Row, ?> expression : expressions) {
                expression.setNextRow(delegate.currentRow());
            }
            currentRow = inputRow;
            return true;
        }
        currentRow = OFF_ROW;
        return false;
    }

    @Override
    public Row currentRow() {
        if (closed) {
            throw new IllegalStateException("BatchIterator is closed");
        }
        return currentRow;
    }

    @Override
    public void close() {
        super.close();
        // this is only checked in currentRow because all other methods delegate to another BatchIterator
        // which itself must raise exceptions if methods are called after close
        closed = true;
    }
}
