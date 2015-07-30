/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.operation.Input;
import io.crate.operation.InputRow;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;

import java.util.List;
import java.util.concurrent.CancellationException;

/**
 * Simple Collector that only collects one row and does not support any query or aggregation
 */
public class SimpleOneRowCollector implements CrateCollector {

    private final InputRow row;
    private final RowDownstreamHandle downstream;
    private volatile boolean killed = false;

    public SimpleOneRowCollector(List<Input<?>> inputs,
                                 RowDownstream downstream) {
        this.row = new InputRow(inputs);
        this.downstream = downstream.registerUpstream(this);
    }

    @Override
    public void doCollect() {
        try {
            downstream.setNextRow(row);
            if (killed) {
                downstream.fail(new CancellationException());
                return;
            }
            downstream.finish();
        } catch (Throwable t) {
            downstream.fail(t);
        }
    }

    @Override
    public void kill() {
        killed = true;
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resume() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void repeat() {

    }
}
