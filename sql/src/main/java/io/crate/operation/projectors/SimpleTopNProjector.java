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
import io.crate.jobs.ExecutionState;
import io.crate.operation.*;
import io.crate.operation.collect.CollectExpression;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SimpleTopNProjector implements Projector, RowUpstream, RowDownstreamHandle {

    private final CollectExpression<Row, ?>[] collectExpressions;
    private final InputRow inputRow;
    private AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private RowDownstreamHandle downstream;

    private int remainingOffset;
    private int toCollect;
    private AtomicReference<Throwable> failure = new AtomicReference<>(null);

    public SimpleTopNProjector(List<Input<?>> inputs,
                               CollectExpression<Row, ?>[] collectExpressions,
                               int limit,
                               int offset) {
        Preconditions.checkArgument(limit >= TopN.NO_LIMIT, "invalid limit");
        Preconditions.checkArgument(offset>=0, "invalid offset");
        this.inputRow = new InputRow(inputs);
        this.collectExpressions = collectExpressions;
        if (limit == TopN.NO_LIMIT) {
            limit = Constants.DEFAULT_SELECT_LIMIT;
        }
        this.remainingOffset = offset;
        this.toCollect = limit;

    }

    @Override
    public void downstream(RowDownstream downstream) {
        this.downstream = downstream.registerUpstream(this);
    }

    @Override
    public void startProjection(ExecutionState executionState) {
    }

    @Override
    public synchronized boolean setNextRow(Row row) {
        if (toCollect<1){
            return false;
        }
        if (remainingOffset > 0) {
            remainingOffset--;
            return true;
        }
        assert downstream != null;
        for (CollectExpression<Row, ?> collectExpression : collectExpressions) {
            collectExpression.setNextRow(row);
        }
        if (!downstream.setNextRow(this.inputRow)) {
            toCollect = -1;
        }
        toCollect--;
        return toCollect > 0 && failure.get() == null;
    }

    @Override
    public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
        remainingUpstreams.incrementAndGet();
        return this;
    }

    @Override
    public void finish() {
        if (remainingUpstreams.decrementAndGet() > 0) {
            return;
        }
        if (downstream != null) {
            Throwable throwable = failure.get();
            if (throwable == null) {
                downstream.finish();
            } else {
                downstream.fail(throwable);
            }
        }
    }

    @Override
    public void fail(Throwable throwable) {
        if (remainingUpstreams.decrementAndGet() <= 0) {
            if (downstream != null) {
                downstream.fail(throwable);
            }
            return;
        }
        failure.set(throwable);
    }
}
