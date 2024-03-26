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

import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.jetbrains.annotations.VisibleForTesting;
import io.crate.common.unit.TimeValue;
import io.crate.expression.reference.sys.job.ContextLog;

final class TimeBasedQEviction {

    private TimeBasedQEviction() {
    }

    static ScheduledFuture<?> scheduleTruncate(long delayInMs,
                                               long intervalInMs,
                                               Queue<? extends ContextLog> q,
                                               ScheduledExecutorService scheduler,
                                               TimeValue expiration) {
        return scheduler.scheduleWithFixedDelay(
            () -> removeExpiredLogs(q, System.currentTimeMillis(), expiration.millis()),
            delayInMs,
            intervalInMs,
            TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    static <T extends ContextLog> void removeExpiredLogs(Queue<T> q, long currentTimeMillis, long expirationTime) {
        long expired = currentTimeMillis - expirationTime;
        for (T t : q) {
            if (t.ended() < expired) {
                q.remove();
            } else {
                break;
            }
        }
    }
}
