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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

public class RamAccountingQueue<T> extends ForwardingQueue<T> {

    private static final ESLogger LOGGER = Loggers.getLogger(StatsTablesService.class);

    private final Queue<T> delegate;
    private final ReentrantLock lock;
    private RamAccountingContext context;
    private final SizeEstimator<T> sizeEstimator;
    private final CircuitBreaker breaker;

    public RamAccountingQueue(Queue<T> delegate, CircuitBreaker breaker, SizeEstimator<T> sizeEstimator) {
        this.delegate = delegate;
        this.breaker = breaker;
        this.sizeEstimator = sizeEstimator;
        this.context = new RamAccountingContext(contextId(), breaker);
        this.lock = new ReentrantLock();
    }

    private static String contextId() {
        return String.format("RamAccountingQueue[%s]", UUID.randomUUID().toString());
    }

    @Override
    public boolean offer(T o) {
        lock.lock();
        try {
            context.addBytesWithoutBreaking(sizeEstimator.estimateSize(o));
            if (context.exceededBreaker()) {
                LOGGER.error("Memory limit for breaker [{}] was exceeded. Queue [{}] is cleared.", breaker.getName(), context.contextId());
                // clear queue, close context and create new one
                close();
                context = new RamAccountingContext(contextId(), breaker);
                // add bytes to new context
                context.addBytesWithoutBreaking(sizeEstimator.estimateSize(o));
            }
        } finally {
            lock.unlock();
        }
        return delegate.offer(o);
    }

    @Override
    protected Queue<T> delegate() {
        return delegate;
    }

    public void close() {
        delegate.clear();
        context.close();
    }
}
