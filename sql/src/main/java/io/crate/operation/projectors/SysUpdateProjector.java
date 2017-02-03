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

package io.crate.operation.projectors;

import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.metadata.expressions.WritableExpression;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.ValueAndInputRow;
import org.elasticsearch.common.collect.Tuple;

/**
 * Projector that executes in-memory updates on the given assignmentExpressions.
 * <p>
 * The rows it receives must be of type {@link ValueAndInputRow}.
 */
class SysUpdateProjector extends AbstractProjector {

    private final Iterable<Tuple<WritableExpression, Input<?>>> assignmentExpressions;
    private final Iterable<? extends CollectExpression<Row, ?>> collectExpressions;

    private long rowCount = 0;

    /**
     * @param assignmentExpressions iterable of tuples
     *                              the tuples contain a writeable expression (target column)
     *                              and the value for the assignment (the source "source")
     *                              <p>
     *                              e.g. [ x = (y + 1) ]
     *                              where
     *                              WriteExpression: x
     *                              Input: (y + 1)
     * @param collectExpressions    expressions that are linked to the "source" inputs of the assignmentExpressions
     */
    SysUpdateProjector(Iterable<Tuple<WritableExpression, Input<?>>> assignmentExpressions,
                       Iterable<? extends CollectExpression<Row, ?>> collectExpressions) {
        this.assignmentExpressions = assignmentExpressions;
        this.collectExpressions = collectExpressions;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Result setNextRow(Row row) {
        assert row instanceof ValueAndInputRow : "row must be instance of ValueAndInputRow";

        for (CollectExpression<Row, ?> collectExpression : collectExpressions) {
            collectExpression.setNextRow(row);
        }
        for (Tuple<WritableExpression, Input<?>> assignmentExpression : assignmentExpressions) {
            WritableExpression writableExpression = assignmentExpression.v1();
            writableExpression.updateValue(((ValueAndInputRow) row).get(), assignmentExpression.v2().value());
        }
        rowCount++;
        return Result.CONTINUE;
    }

    @Override
    public void finish(RepeatHandle repeatHandle) {
        downstream.setNextRow(new Row1(rowCount));
        downstream.finish(RepeatHandle.UNSUPPORTED);
        ;
    }

    @Override
    public void fail(Throwable throwable) {
        downstream.fail(throwable);
    }
}
