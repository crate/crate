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

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.ToLongFunction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.breaker.CircuitBreaker;

import io.crate.breaker.ConcurrentRamAccounting;
import io.crate.data.breaker.RamAccounting;

public final class RamAccountingQueue<T> implements Queue<T> {

    private static final Logger LOGGER = LogManager.getLogger(RamAccountingQueue.class);

    private final Queue<T> delegate;
    private final RamAccounting ramAccounting;
    private final ToLongFunction<T> getElementSize;
    private final CircuitBreaker breaker;
    private final AtomicBoolean exceeded;

    public RamAccountingQueue(Queue<T> delegate, CircuitBreaker breaker, ToLongFunction<T> getElementSize) {
        this.delegate = delegate;
        this.breaker = breaker;
        this.getElementSize = getElementSize;
        // create a non-breaking (thread-safe) instance as this component will check the breaker limits by itself
        this.ramAccounting = new ConcurrentRamAccounting(
            breaker::addWithoutBreaking,
            bytes -> breaker.addWithoutBreaking(- bytes),
            breaker.getName(),
            0
        );
        this.exceeded = new AtomicBoolean(false);
    }

    private static String contextId() {
        return "RamAccountingQueue[" + UUIDs.dirtyUUID() + ']';
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return delegate.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        return delegate.iterator();
    }

    @Override
    public Object[] toArray() {
        return delegate.toArray();
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        return delegate.toArray(a);
    }

    @Override
    public boolean add(T t) {
        return delegate.add(t);
    }

    @Override
    public boolean remove(Object o) {
        return delegate.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return delegate.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return delegate.addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return delegate.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return delegate.removeAll(c);
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public boolean offer(T o) {
        long elementSize = getElementSize.applyAsLong(o);
        ramAccounting.addBytes(elementSize);
        if (exceededBreaker() && exceeded.compareAndSet(false, true)) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Memory limit for breaker [{}] was exceeded. Queue [{}] is cleared.", breaker.getName(), contextId());
            }
            release();
            ramAccounting.addBytes(elementSize);
            exceeded.set(false);
        }
        return delegate.offer(o);
    }

    @Override
    public T remove() {
        return delegate.remove();
    }

    @Override
    public T poll() {
        return delegate.poll();
    }

    @Override
    public T element() {
        return delegate.element();
    }

    @Override
    public T peek() {
        return delegate.peek();
    }

    public void release() {
        delegate.clear();
        ramAccounting.release();
    }

    /**
     * Returns true if the limit of the breaker was already reached
     * but the breaker did not trip (e.g. when adding bytes without breaking)
     */
    public boolean exceededBreaker() {
        return breaker.getUsed() >= breaker.getLimit();
    }
}
