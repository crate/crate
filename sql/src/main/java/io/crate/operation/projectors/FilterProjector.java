/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.projectors;

import io.crate.core.collections.Row;
import io.crate.jobs.ExecutionState;
import io.crate.operation.Input;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;
import io.crate.operation.collect.CollectExpression;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class FilterProjector extends RowDownstreamAndHandle implements Projector {

    private final RowFilter<Row> rowFilter;

    private RowDownstreamHandle downstream;
    private AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private final AtomicReference<Throwable> upstreamFailure = new AtomicReference<>(null);

    public FilterProjector(Collection<CollectExpression<Row, ?>> collectExpressions,
                           Input<Boolean> condition) {
        rowFilter = new RowFilter<>(collectExpressions, condition);
    }

    @Override
    public void startProjection(ExecutionState executionState) {
    }

    @Override
    public synchronized boolean setNextRow(Row row) {
        if (downstream == null) {
            throw new IllegalStateException("setNextRow called on FilterProjector without downstream");
        }
        
        //noinspection SimplifiableIfStatement
        if (rowFilter.matches(row)) {
            return downstream.setNextRow(row);
        }
        return true;
    }

    @Override
    public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
        remainingUpstreams.incrementAndGet();
        return super.registerUpstream(upstream);
    }

    @Override
    public void finish() {
        if (remainingUpstreams.decrementAndGet() > 0) {
            return;
        }
        if (downstream != null) {
            Throwable throwable = upstreamFailure.get();
            if (throwable == null) {
                downstream.finish();
            } else {
                downstream.fail(throwable);
            }
        }
    }

    @Override
    public void fail(Throwable throwable) {
        upstreamFailure.set(throwable);
        if (remainingUpstreams.decrementAndGet() > 0) {
            return;
        }
        if (downstream != null) {
            downstream.fail(throwable);
        }
    }

    @Override
    public void downstream(RowDownstream downstream) {
        this.downstream = downstream.registerUpstream(this);
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resume(boolean async) {
        throw new UnsupportedOperationException();
    }
}
