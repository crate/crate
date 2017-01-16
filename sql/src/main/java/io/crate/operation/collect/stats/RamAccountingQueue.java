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

import com.google.common.collect.ForwardingQueue;
import io.crate.breaker.RamAccountingContext;
import io.crate.breaker.SizeEstimator;

import java.util.NoSuchElementException;
import java.util.Queue;

public class RamAccountingQueue<T> extends ForwardingQueue<T> {

    private final Queue<T> delegate;
    private final RamAccountingContext context;
    private final SizeEstimator<T> sizeEstimator;

    public RamAccountingQueue(Queue<T> delegate, RamAccountingContext context, SizeEstimator<T> sizeEstimator) {
        this.delegate = delegate;
        this.context = context;
        this.sizeEstimator = sizeEstimator;
    }

    @Override
    public boolean offer(T o) {
        context.addBytesWithoutBreaking(sizeEstimator.estimateSize(o));
        if (context.exceededBreaker()) {
            try {
                delegate.remove();
            } catch (NoSuchElementException ignored) {
                // queue empty
            }
        }
        return delegate.offer(o);
    }

    @Override
    protected Queue<T> delegate() {
        return delegate;
    }

    public void close() {
        for (T t : delegate) {
            context.addBytesWithoutBreaking(-sizeEstimator.estimateSize(t));
        }
        delegate.clear();
    }
}
