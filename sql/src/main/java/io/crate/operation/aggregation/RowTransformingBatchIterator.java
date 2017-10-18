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
import java.util.RandomAccess;

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
public class RowTransformingBatchIterator extends ForwardingBatchIterator<Row> {

    private final BatchIterator<Row> delegate;
    private final List<? extends CollectExpression<Row, ?>> expressions;
    private final Row rowData;

    public RowTransformingBatchIterator(BatchIterator<Row> delegate,
                                        List<? extends Input<?>> inputs,
                                        List<? extends CollectExpression<Row, ?>> expressions) {
        assert expressions instanceof RandomAccess
            : "Must be able to use fast indexed for loop to avoid iterator allocations";

        this.delegate = delegate;
        this.expressions = expressions;
        this.rowData = new InputRow(inputs);
    }

    @Override
    public Row currentElement() {
        return rowData;
    }

    @Override
    protected BatchIterator<Row> delegate() {
        return delegate;
    }

    @Override
    public boolean moveNext() {
        if (delegate.moveNext()) {
            Row row = delegate.currentElement();
            for (int i = 0; i < expressions.size(); i++) {
                expressions.get(i).setNextRow(row);
            }
            return true;
        }
        return false;
    }
}
