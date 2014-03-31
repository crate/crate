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
import io.crate.operation.Input;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.collect.CollectExpression;

import java.util.concurrent.atomic.AtomicInteger;

public class SimpleTopNProjector implements Projector {

    private final Input<?>[] inputs;
    private final CollectExpression<?>[] collectExpressions;
    private AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private Projector downstream;

    private int remainingOffset;
    private int toCollect;

    public SimpleTopNProjector(Input<?>[] inputs,
                               CollectExpression<?>[] collectExpressions,
                               int limit,
                               int offset) {
        Preconditions.checkArgument(limit >= TopN.NO_LIMIT, "invalid limit");
        Preconditions.checkArgument(offset>=0, "invalid offset");
        this.inputs = inputs;
        this.collectExpressions = collectExpressions;
        if (limit == TopN.NO_LIMIT) {
            limit = Constants.DEFAULT_SELECT_LIMIT;
        }
        this.remainingOffset = offset;
        this.toCollect = limit;
    }

    @Override
    public void downstream(Projector downstream) {
        this.downstream = downstream;
        downstream.registerUpstream(this);
    }

    @Override
    public Projector downstream() {
        return downstream;
    }

    @Override
    public void startProjection() {
        if (remainingUpstreams.get() <= 0) {
            upstreamFinished();
        }
    }

    @Override
    public synchronized boolean setNextRow(Object[] row) {
        assert toCollect >= 1;
        if (remainingOffset > 0) {
            remainingOffset--;
            return true;
        }

        Object[] evaluatedRow = generateNextRow(row);
        if (downstream != null) {
            if (!downstream.setNextRow(evaluatedRow)) {
                toCollect = -1;
            }
        }

        toCollect--;
        return toCollect > 0;
    }

    private Object[] generateNextRow(Object[] row) {
        Object[] evaluatedRow = new Object[inputs.length];
        for (CollectExpression<?> collectExpression : collectExpressions) {
            collectExpression.setNextRow(row);
        }
        int i = 0;
        for (Input<?> input : inputs) {
            evaluatedRow[i++] = input.value();
        }
        return evaluatedRow;
    }

    @Override
    public void registerUpstream(ProjectorUpstream upstream) {
        remainingUpstreams.incrementAndGet();
    }

    @Override
    public void upstreamFinished() {
        if (remainingUpstreams.decrementAndGet() > 0) {
            return;
        }
        if (downstream != null) {
            downstream.upstreamFinished();
        }
    }
}
