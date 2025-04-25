/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.dml;

import java.util.Collections;
import java.util.function.Consumer;
import java.util.stream.Collector;

import io.crate.data.BatchIterator;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.data.Row1;

public class SysUpdateProjector implements Projector {

    private final Consumer<Object> rowWriter;

    public SysUpdateProjector(Consumer<Object> rowWriter) {
        this.rowWriter = rowWriter;
    }

    @Override
    public BatchIterator<Row> apply(BatchIterator<Row> batchIterator) {
        return CollectingBatchIterator.newInstance(batchIterator,
            Collector.of(
                () -> new State(rowWriter),
                (state, row) -> {
                    state.rowWriter.accept(row.get(0));
                    state.rowCount++;
                },
                (_, _) -> {
                    throw new UnsupportedOperationException("Combine not supported");
                },
                state -> Collections.singletonList(new Row1(state.rowCount))
            ));
    }

    @Override
    public boolean providesIndependentScroll() {
        return true;
    }

    private static class State {
        final Consumer<Object> rowWriter;
        long rowCount = 0;

        State(Consumer<Object> rowWriter) {
            this.rowWriter = rowWriter;
        }
    }
}
