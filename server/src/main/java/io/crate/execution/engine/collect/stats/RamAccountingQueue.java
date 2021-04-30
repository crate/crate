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

import com.google.common.collect.ForwardingQueue;
import io.crate.breaker.ConcurrentRamAccounting;
import io.crate.breaker.RamAccounting;
import io.crate.breaker.SizeEstimator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.breaker.CircuitBreaker;

import java.util.Locale;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class RamAccountingQueue<T> extends ForwardingQueue<T> {

    private static final Logger LOGGER = LogManager.getLogger(RamAccountingQueue.class);

    private final Queue<T> delegate;
    private final RamAccounting ramAccounting;
    private final SizeEstimator<T> sizeEstimator;
    private final CircuitBreaker breaker;
    private final AtomicBoolean exceeded;

    public RamAccountingQueue(Queue<T> delegate, CircuitBreaker breaker, SizeEstimator<T> sizeEstimator) {
        this.delegate = delegate;
        this.breaker = breaker;
        this.sizeEstimator = sizeEstimator;
        // create a non-breaking (thread-safe) instance as this component will check the breaker limits by itself
        this.ramAccounting = new ConcurrentRamAccounting(
            breaker::addWithoutBreaking,
            bytes -> breaker.addWithoutBreaking(- bytes)
        );
        this.exceeded = new AtomicBoolean(false);
    }

    private static String contextId() {
        return String.format(Locale.ENGLISH, "RamAccountingQueue[%s]", UUID.randomUUID().toString());
    }

    @Override
    public boolean offer(T o) {
        ramAccounting.addBytes(sizeEstimator.estimateSize(o));
        if (exceededBreaker() && exceeded.compareAndSet(false, true)) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Memory limit for breaker [{}] was exceeded. Queue [{}] is cleared.", breaker.getName(), contextId());
            }
            release();
            ramAccounting.addBytes(sizeEstimator.estimateSize(o));
            exceeded.set(false);
        }
        return delegate.offer(o);
    }

    @Override
    protected Queue<T> delegate() {
        return delegate;
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
