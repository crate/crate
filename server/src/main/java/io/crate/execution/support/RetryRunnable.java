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

package io.crate.execution.support;

import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.common.unit.TimeValue;

public class RetryRunnable implements Runnable, Scheduler.Cancellable {

    private static final Logger LOGGER = LogManager.getLogger(RetryRunnable.class);

    private final ThreadPool threadPool;
    private final String executorName;
    private final Executor executor;
    private final Iterator<TimeValue> delay;
    private final Runnable retryCommand;
    private volatile Scheduler.Cancellable cancellable;

    public RetryRunnable(ThreadPool threadPool,
                         String executorName,
                         Runnable command,
                         Iterable<TimeValue> backOffPolicy) {
        this.threadPool = threadPool;
        this.executorName = executorName;
        this.executor = threadPool.executor(executorName);
        this.delay = backOffPolicy.iterator();
        this.retryCommand = command;
    }

    @Override
    public void run() {
        try {
            executor.execute(retryCommand);
        } catch (RejectedExecutionException e) {
            if (delay.hasNext()) {
                long currentDelay = delay.next().millis();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Received RejectedExecutionException, will retry again in {}ms", currentDelay);
                }
                cancellable = threadPool.scheduleUnlessShuttingDown(
                    new TimeValue(currentDelay, TimeUnit.MILLISECONDS),
                    executorName,
                    retryCommand
                );
            } else {
                LOGGER.warn("Received RejectedExecutionException after max retries, giving up");
            }
        } catch (Exception e) {
            LOGGER.error("Received unhandled exception", e);
        }
    }

    @Override
    public boolean cancel() {
        if (cancellable != null) {
            return cancellable.cancel();
        }
        return false;
    }

    @Override
    public boolean isCancelled() {
        if (cancellable != null) {
            return cancellable.isCancelled();
        }
        return false;
    }
}
