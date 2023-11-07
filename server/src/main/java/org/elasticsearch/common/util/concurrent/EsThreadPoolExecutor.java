/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util.concurrent;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.crate.common.SuppressForbidden;

/**
 * An extension to thread pool executor, allowing (in the future) to add specific additional stats to it.
 */
public class EsThreadPoolExecutor extends ThreadPoolExecutor {

    /**
     * Name used in error reporting.
     */
    private final String name;

    final String getName() {
        return name;
    }

    EsThreadPoolExecutor(String name,
                         int corePoolSize,
                         int maximumPoolSize,
                         long keepAliveTime,
                         TimeUnit unit,
                         BlockingQueue<Runnable> workQueue,
                         ThreadFactory threadFactory) {
        this(name, corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, new EsAbortPolicy());
    }

    @SuppressForbidden(reason = "properly rethrowing errors, see EsExecutors.rethrowErrors")
    EsThreadPoolExecutor(String name,
                         int corePoolSize,
                         int maximumPoolSize,
                         long keepAliveTime,
                         TimeUnit unit,
                         BlockingQueue<Runnable> workQueue,
                         ThreadFactory threadFactory,
                         XRejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
        this.name = name;
    }

    @Override
    public void execute(Runnable command) {
        command = wrapRunnable(command);
        try {
            super.execute(command);
        } catch (EsRejectedExecutionException ex) {
            if (command instanceof AbstractRunnable runnable) {
                // If we are an abstract runnable we can handle the rejection
                // directly and don't need to rethrow it.
                try {
                    runnable.onRejection(ex);
                } finally {
                    runnable.onAfter();

                }
            } else {
                throw ex;
            }
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        EsExecutors.rethrowErrors(unwrap(r));
    }


    @Override
    public final String toString() {
        StringBuilder b = new StringBuilder();
        b.append(getClass().getSimpleName()).append('[');
        b.append("name = ").append(name).append(", ");
        if (getQueue() instanceof SizeBlockingQueue) {
            @SuppressWarnings("rawtypes")
            SizeBlockingQueue queue = (SizeBlockingQueue) getQueue();
            b.append("queue capacity = ").append(queue.capacity()).append(", ");
        }
        /*
         * ThreadPoolExecutor has some nice information in its toString but we
         * can't get at it easily without just getting the toString.
         */
        b.append(super.toString()).append(']');
        return b.toString();
    }

    protected Runnable wrapRunnable(Runnable command) {
        return command;
    }

    protected Runnable unwrap(Runnable runnable) {
        while (runnable instanceof WrappedRunnable wrapped) {
            runnable = wrapped.unwrap();
        }
        return runnable;
    }
}
