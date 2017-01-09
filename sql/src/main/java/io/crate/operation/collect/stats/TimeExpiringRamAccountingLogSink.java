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

import com.google.common.annotations.VisibleForTesting;
import io.crate.breaker.RamAccountingContext;
import io.crate.operation.reference.sys.job.ContextLog;
import org.elasticsearch.common.unit.TimeValue;

import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class TimeExpiringRamAccountingLogSink<T extends ContextLog> extends RamAccountingLogSink<T> {

    private static final long QUEUE_CLEAN_INTERVAL = 5L; // in seconds

    private final ScheduledFuture scheduledFuture;

    public TimeExpiringRamAccountingLogSink(RamAccountingContext context, ScheduledExecutorService scheduler, TimeValue expiration, Function<T, Long> estimatorFunction) {
        super(new ConcurrentLinkedDeque<T>(), context, estimatorFunction);
        scheduledFuture = scheduler.scheduleWithFixedDelay(
            () -> removeExpiredLogs(System.currentTimeMillis(), expiration.getMillis()),
            0L, QUEUE_CLEAN_INTERVAL, TimeUnit.SECONDS);
    }

    private T remove() {
        T removed = queue.remove();
        context.addBytesWithoutBreaking(-estimatorFunction.apply(removed));
        return removed;
    }

    @Override
    public void close() {
        // Cancel the current jobs log queue scheduler if there is one
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
        super.close();
    }

    @VisibleForTesting
    void removeExpiredLogs(long currentTimeMillis, long expirationTime) {
        long expired = currentTimeMillis - expirationTime;
        for (T t : queue) {
            if (t.ended() < expired) {
                queue.remove();
            } else {
                break;
            }
        }
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
            } catch (NoSuchElementException e) {
                // in case the queue is empty
            }
        }
        queue.offer(item);
    }

}
