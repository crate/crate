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
import io.crate.data.*;
import io.crate.operation.aggregation.RowTransformingBatchIterator;
import io.crate.operation.collect.CollectExpression;

import java.util.List;

public class SimpleTopNProjector implements Projector {

    private final List<Input<?>> inputs;
    private final Iterable<? extends CollectExpression<Row, ?>> collectExpressions;
    private final int offset;
    private final int limit;

    public SimpleTopNProjector(List<Input<?>> inputs,
                               Iterable<? extends CollectExpression<Row, ?>> collectExpressions,
                               int limit,
                               int offset) {
        Preconditions.checkArgument(limit >= 0, "invalid limit: " + limit);
        Preconditions.checkArgument(offset >= 0, "invalid offset: " + offset);

        this.inputs = inputs;
        this.collectExpressions = collectExpressions;
        this.limit = limit;
        this.offset = offset;
    }

    @Override
    public BatchIteratorProjector asProjector() {
        return it -> {
            if (it == null) {
                return null;
            }
            if (offset > 0) {
                it = new SkippingBatchIterator(it, offset);
            }
            return new RowTransformingBatchIterator(
                    LimitingBatchIterator.newInstance(it, limit),
                    inputs,
                    collectExpressions
            );
        };
    }
}
