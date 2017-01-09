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

import io.crate.breaker.RamAccountingContext;

import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Function;

public class FixedSizeRamAccountingLogSink<T> extends RamAccountingLogSink<T> {

    public FixedSizeRamAccountingLogSink(RamAccountingContext context, int size, Function<T, Long> estimatorFunction) {
        super(new ArrayBlockingQueue<>(size), context, estimatorFunction);
        assert size > 0 : "capacity should be > 0";
    }

    @Override
    public void add(T item) {
        context.addBytesWithoutBreaking(estimatorFunction.apply(item));
        if (context.exceededBreaker()) {
            /*
             * remove last entry from queue in order to fit in new job
             * we don't care if the last entry was a bit smaller than the new one, it's only a few bytes anyway
             */
            try {
                remove();
            } catch (NoSuchElementException ex) {
                // in case queue was already empty
            }
        }
        while (true) {
            if (!queue.offer(item)) {
                try {
                    remove();
                } catch (NoSuchElementException ex) {
                    // race condition, queue was already empty
                }
            } else {
                break;
            }
        }
    }

    private T remove() {
        T removed = queue.remove();
        context.addBytesWithoutBreaking(-estimatorFunction.apply(removed));
        return removed;
    }
}
