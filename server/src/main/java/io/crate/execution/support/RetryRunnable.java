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

import io.crate.common.unit.TimeValue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RetryRunnable implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger(RetryRunnable.class);

    private final Executor executor;
    private final ScheduledExecutorService scheduler;
    private final Iterator<TimeValue> delay;
    private final Runnable retryCommand;

    public RetryRunnable(Executor executor,
                         ScheduledExecutorService scheduler,
                         Runnable command,
                         Iterable<TimeValue> backOffPolicy) {
        this.executor = executor;
        this.scheduler = scheduler;
        this.delay = backOffPolicy.iterator();
        this.retryCommand = command;
    }

    @Override
    public void run() {
        try {
            executor.execute(retryCommand);
        } catch (EsRejectedExecutionException e) {
            if (delay.hasNext()) {
                long currentDelay = delay.next().millis();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Received RejectedExecutionException, will retry again in {}ms", currentDelay);
                }
                scheduler.schedule(retryCommand, currentDelay, TimeUnit.MILLISECONDS);
            } else {
                LOGGER.warn("Received RejectedExecutionException after max retries, giving up");
            }
        } catch (Exception e) {
            LOGGER.error("Received unhandled exception", e);
        }
    }
}
