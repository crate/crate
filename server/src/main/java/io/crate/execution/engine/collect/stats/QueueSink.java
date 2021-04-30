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

package io.crate.execution.engine.collect.stats;

import java.util.Iterator;
import java.util.Queue;

public class QueueSink<T> implements LogSink<T> {

    private final Queue<T> queue;
    private final Runnable onClose;

    public QueueSink(Queue<T> queue, Runnable onClose) {
        this.queue = queue;
        this.onClose = onClose;
    }

    @Override
    public void add(T item) {
        queue.offer(item);
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
        onClose.run();
    }
}
