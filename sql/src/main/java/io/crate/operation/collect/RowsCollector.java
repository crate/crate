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
import io.crate.core.collections.Row;
import io.crate.operation.data.BatchConsumer;

import javax.annotation.Nullable;

public class RowsCollector implements CrateCollector {

    private final BatchConsumer downstream;
    private final Iterable<Row> rows;

    public static RowsCollector empty(BatchConsumer rowDownstream) {
        return new RowsCollector(rowDownstream, ImmutableList.of());
    }

    public static RowsCollector single(Row row, BatchConsumer rowDownstream) {
        return new RowsCollector(rowDownstream, ImmutableList.of(row));
    }

    public RowsCollector(BatchConsumer downstream, Iterable<Row> rows) {
        this.downstream = downstream;
        this.rows = rows;
    }

    @Override
    public void doCollect() {
        downstream.accept(new RowBasedBatchCursor(rows), null);
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
        // XDOBE: impl kill
    }

    public static Builder emptyBuilder() {
        return new CrateCollector.Builder() {
            @Override
            public CrateCollector build(BatchConsumer rowReceiver) {
                return RowsCollector.empty(rowReceiver);
            }
        };
    }

    public static Builder builder(final Iterable<Row> rows) {
        return new CrateCollector.Builder() {
            @Override
            public CrateCollector build(BatchConsumer rowReceiver) {
                return new RowsCollector(rowReceiver, rows);
            }
        };
    }

}
