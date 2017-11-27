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

package io.crate.executor.task;

import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.crate.data.SentinelRow.SENTINEL;

public class NoopTask {

    public static final NoopTask INSTANCE = new NoopTask();

    private NoopTask() {
    }

    public void execute(RowConsumer consumer) {
        consumer.accept(InMemoryBatchIterator.empty(SENTINEL), null);
    }

    public List<CompletableFuture<Long>> executeBulk(List<Row> bulkParams) {
        return Collections.emptyList();
    }
}
