/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.projectors;

import io.crate.data.BatchIteratorProjector;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.function.Consumer;
import java.util.stream.Collector;

class SysUpdateProjector extends AbstractProjector {

    private final Consumer<Object> rowWriter;

    SysUpdateProjector(Consumer<Object> rowWriter)  {
        this.rowWriter = rowWriter;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Result setNextRow(Row row) {
        throw new UnsupportedOperationException("SysUpdateProjector should be used as BatchIteratorProjector");
    }

    @Override
    public void finish(RepeatHandle repeatHandle) {
        throw new UnsupportedOperationException("SysUpdateProjector should be used as BatchIteratorProjector");
    }

    @Override
    public void fail(Throwable throwable) {
        throw new UnsupportedOperationException("SysUpdateProjector should be used as BatchIteratorProjector");
    }

    @Nullable
    @Override
    public BatchIteratorProjector asProjector() {
        return bi -> CollectingBatchIterator.newInstance(bi,
            Collector.of(
                () -> new State(rowWriter),
                (state, row) -> {
                    state.rowWriter.accept(row.get(0));
                    state.rowCount++;
                },
                (state1, state2) -> { throw new UnsupportedOperationException("Combine not supported"); },
                state -> Collections.singletonList(new Row1(state.rowCount))
            ), 1);
    }

    private static class State {
        final Consumer<Object> rowWriter;
        long rowCount = 0;

        State(Consumer<Object> rowWriter) {
            this.rowWriter = rowWriter;
        }
    }
}
