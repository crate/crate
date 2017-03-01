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

import com.google.common.collect.ImmutableList;
import io.crate.data.Row;
import io.crate.operation.projectors.IterableRowEmitter;
import io.crate.operation.projectors.RowReceiver;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class RowsCollector implements CrateCollector {

    private final RowReceiver rowDownstream;
    private final CompletableFuture<? extends Iterable<?>> future;
    private final Function<Iterable, Iterable<? extends Row>> dataIterableToRowsIterable;
    private IterableRowEmitter emitter;

    public static RowsCollector empty(RowReceiver rowDownstream) {
        return new RowsCollector(
            rowDownstream,
            CompletableFuture.completedFuture(ImmutableList.of()),
            dataIterable -> ImmutableList.of());
    }

    public static RowsCollector single(Row row, RowReceiver rowDownstream) {
        return new RowsCollector(
            rowDownstream,
            CompletableFuture.completedFuture(ImmutableList.of(row)),
            dataIterable -> (Iterable<? extends Row>) dataIterable);
    }

    public RowsCollector(RowReceiver rowDownstream,
                         CompletableFuture<? extends Iterable<?>> future,
                         Function<Iterable, Iterable<? extends Row>> dataIterableToRowsIterable) {
        this.rowDownstream = rowDownstream;
        this.future = future;
        this.dataIterableToRowsIterable = dataIterableToRowsIterable;
    }

    @Override
    public void doCollect() {
        future.whenComplete((objects, throwable) -> {
            if (throwable == null) {
                emitter = new IterableRowEmitter(rowDownstream, dataIterableToRowsIterable.apply(objects));
                emitter.run();
            } else {
                rowDownstream.fail(throwable);
            }
        });
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
        if (emitter != null) {
            emitter.kill(throwable);
        }
    }

    static Builder emptyBuilder() {
        return RowsCollector::empty;
    }

    public static Builder builder(Iterable<Row> rows) {
        return rowReceiver -> new RowsCollector(
            rowReceiver,
            CompletableFuture.completedFuture(rows),
            (Function<Iterable, Iterable<? extends Row>>) dataIterable -> (Iterable<? extends Row>) dataIterable);
    }
}
