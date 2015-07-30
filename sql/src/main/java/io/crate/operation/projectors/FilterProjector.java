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
import io.crate.operation.*;
import io.crate.operation.collect.CollectExpression;

import java.util.Collection;

public class FilterProjector implements Projector, RowDownstreamHandle {

    private final RowFilter<Row> rowFilter;
    private final MultiUpstreamRowDownstream multiUpstreamRowDownstream = new MultiUpstreamRowDownstream();
    private final MultiUpstreamRowUpstream multiUpstreamRowUpstream = new MultiUpstreamRowUpstream(multiUpstreamRowDownstream);

    public FilterProjector(Collection<CollectExpression<Row, ?>> collectExpressions,
                           Input<Boolean> condition) {
        rowFilter = new RowFilter<>(collectExpressions, condition);
    }

    @Override
    public void startProjection(ExecutionState executionState) {
    }

    @Override
    public synchronized boolean setNextRow(Row row) {
        if (multiUpstreamRowDownstream.downstreamHandle() == null) {
            throw new IllegalStateException("setNextRow called on FilterProjector without downstream");
        }

        //noinspection SimplifiableIfStatement
        if (rowFilter.matches(row)) {
            return multiUpstreamRowDownstream.setNextRow(row);
        }
        return true;
    }

    @Override
    public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
        multiUpstreamRowDownstream.registerUpstream(upstream);
        return this;
    }

    @Override
    public void finish() {
        multiUpstreamRowDownstream.finish();
    }

    @Override
    public void fail(Throwable throwable) {
        multiUpstreamRowDownstream.fail(throwable);
    }

    @Override
    public void downstream(RowDownstream downstream) {
        multiUpstreamRowDownstream.downstreamHandle(downstream.registerUpstream(this));
    }

    @Override
    public void pause() {
        multiUpstreamRowUpstream.pause();
    }

    @Override
    public void resume() {
        multiUpstreamRowUpstream.resume();
    }
}
