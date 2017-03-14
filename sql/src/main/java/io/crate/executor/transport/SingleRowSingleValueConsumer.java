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

package io.crate.executor.transport;

import io.crate.data.CollectingBatchConsumer;
import io.crate.data.Row;

import java.util.stream.Collector;

/**
 * BatchConsumer expects to receive only one row and triggers a future with the value once completed
 */
class SingleRowSingleValueConsumer {

    private final static Object SENTINEL = new Object();
    private static final Collector<Row, Object[], Object> SINGLE_VALUE_COLLECTOR = Collector.of(
        () -> new Object[]{SENTINEL},
        (state, row) -> {
            if (state[0] == SENTINEL) {
                state[0] = row.get(0);
            } else {
                throw new UnsupportedOperationException("Subquery returned more than 1 row");
            }
        },
        (state1, state2) -> {
            throw new UnsupportedOperationException("Combine not supported");
        },
        state -> {
            if (state[0] == SENTINEL) {
                return null;
            } else {
                return state[0];
            }
        }
    );

    public static CollectingBatchConsumer<Object[], Object> create() {
        return new CollectingBatchConsumer<>(SINGLE_VALUE_COLLECTOR);
    }
}
