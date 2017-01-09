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

package io.crate.operation.collect.stats;

import com.google.common.annotations.VisibleForTesting;
import io.crate.breaker.RamAccountingContext;

import java.util.Iterator;
import java.util.Queue;
import java.util.function.Function;

/**
 * A LogSink that accounts memory to circuit breaker when adding items.
 * The sink accepts a queue implementation and a ram accounting context.
 * The queue maintains that list of items.
 * The ram accounting context takes care of the used bytes for the circuit breaker.
 * @param <T> Type of the items in the sink.
 */
public abstract class RamAccountingLogSink<T> implements LogSink<T> {

    final Queue<T> queue;
    final RamAccountingContext context;
    final Function<T, Long> estimatorFunction;

    RamAccountingLogSink(Queue<T> queue, RamAccountingContext context, Function<T, Long> estimatorFunction) {
        this.queue = queue;
        this.context = context;
        this.estimatorFunction = estimatorFunction;
    }

    @Override
    public void addAll(Iterable<T> iterable) {
        for (T t : iterable) {
            add(t);
        }
    }

    @Override
    public Iterator<T> iterator() {
        return queue.iterator();
    }

    @Override
    public void close() {
        for (T t : queue) {
            context.addBytesWithoutBreaking(-estimatorFunction.apply(t));
        }
        queue.clear();
    }

    @VisibleForTesting
    int size() {
        return queue.size();
    }
}
