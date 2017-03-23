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
import io.crate.core.collections.Row;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectExpression;

import java.util.List;

public class SimpleTopNProjector extends InputRowProjector {

    private int remainingOffset;
    private int toCollect;

    public SimpleTopNProjector(List<Input<?>> inputs,
                               Iterable<? extends CollectExpression<Row, ?>> collectExpressions,
                               int limit,
                               int offset) {
        super(inputs, collectExpressions);
        Preconditions.checkArgument(limit >= 0, "Invalid LIMIT: value must be >= 0; got: " + limit);
        Preconditions.checkArgument(offset >= 0, "Invalid OFFSET: value must be >= 0; got: " + offset);
        this.remainingOffset = offset;
        this.toCollect = limit;
    }

    @Override
    public Result setNextRow(Row row) {
        if (toCollect < 1) {
            return Result.STOP;
        }
        if (remainingOffset > 0) {
            remainingOffset--;
            return Result.CONTINUE;
        }
        toCollect--;
        Result result = super.setNextRow(row);
        switch (result) {
            case PAUSE:
                return result;
            case CONTINUE:
                return toCollect < 1 ? Result.STOP : Result.CONTINUE;
            case STOP:
                toCollect = -1;
                return Result.STOP;
        }
        throw new AssertionError("Unrecognized setNextRow result: " + result);
    }
}
