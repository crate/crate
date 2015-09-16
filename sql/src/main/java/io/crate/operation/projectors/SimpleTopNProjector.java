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

package io.crate.operation.projectors;

import com.google.common.base.Preconditions;
import io.crate.Constants;
import io.crate.core.collections.Row;
import io.crate.operation.Input;
import io.crate.operation.InputRow;
import io.crate.operation.collect.CollectExpression;

import java.util.List;

public class SimpleTopNProjector extends AbstractProjector {

    private final InputRow inputRow;
    private final Iterable<? extends CollectExpression<Row, ?>> collectExpressions;

    private int remainingOffset;
    private int toCollect;

    public SimpleTopNProjector(List<Input<?>> inputs,
                               Iterable<? extends CollectExpression<Row, ?>> collectExpressions,
                               int limit,
                               int offset) {
        this.collectExpressions = collectExpressions;
        Preconditions.checkArgument(limit >= TopN.NO_LIMIT, "invalid limit");
        Preconditions.checkArgument(offset>=0, "invalid offset");
        this.inputRow = new InputRow(inputs);
        if (limit == TopN.NO_LIMIT) {
            limit = Constants.DEFAULT_SELECT_LIMIT;
        }
        this.remainingOffset = offset;
        this.toCollect = limit;

    }

    @Override
    public boolean setNextRow(Row row) {
        if (toCollect < 1){
            return false;
        }
        if (remainingOffset > 0) {
            remainingOffset--;
            return true;
        }
        for (CollectExpression<Row, ?> collectExpression : collectExpressions) {
            collectExpression.setNextRow(row);
        }
        if (!downstream.setNextRow(this.inputRow)) {
            toCollect = -1;
            return false;
        } else {
            toCollect--;
            return toCollect > 0;
        }
    }

    @Override
    public void finish() {
        downstream.finish();
    }

    @Override
    public void fail(Throwable throwable) {
        downstream.fail(throwable);
    }
}
