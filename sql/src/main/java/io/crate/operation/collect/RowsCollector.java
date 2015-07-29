/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.collect;

import io.crate.operation.*;
import io.crate.operation.projectors.RowFilter;

import java.util.List;
import java.util.concurrent.CancellationException;

public class RowsCollector<R> implements CrateCollector, RowUpstream {

    private final Iterable<R> rows;
    private final RowDownstreamHandle rowDownstream;
    private final InputRow row;
    private final RowFilter<R> rowFilter;
    private volatile boolean killed;

    public RowsCollector(List<Input<?>> inputs,
                         List<CollectExpression<R, ?>> collectExpressions,
                         RowDownstream rowDownstream,
                         Iterable<R> rows,
                         Input<Boolean> condition) {
        this.row = new InputRow(inputs);
        this.rows = rows;
        this.rowFilter = new RowFilter<>(collectExpressions, condition);
        this.rowDownstream = rowDownstream.registerUpstream(this);
    }

    @Override
    public void doCollect() {
        try {
            for (R row : rows) {
                if (killed) {
                    rowDownstream.fail(new CancellationException());
                    return;
                }
                if (rowFilter.matches(row)) {
                    if (!rowDownstream.setNextRow(this.row)) {
                        // no more rows required, we can stop here
                        break;
                    }
                }
            }
            rowDownstream.finish();
        } catch (Throwable t) {
            rowDownstream.fail(t);
        }
    }

    @Override
    public void kill() {
        killed = true;
    }
}
