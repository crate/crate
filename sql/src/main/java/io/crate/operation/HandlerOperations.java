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
package io.crate.operation;

import com.google.common.util.concurrent.MoreExecutors;
import io.crate.concurrent.CompletionListener;
import io.crate.concurrent.CompletionState;
import io.crate.jobs.JobContextService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.Consumer;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Singleton
public class HandlerOperations {

    private static final TimeValue REAPER_INTERVAL = TimeValue.timeValueSeconds(30);
    final Map<UUID, Context> cursorIdToReceiverMap = new ConcurrentHashMap<>();

    @Inject
    public HandlerOperations(ThreadPool threadPool, final JobContextService jobContextService) {
        this(threadPool.scheduler(),
            new Consumer<UUID>() {
                @Override
                public void accept(UUID jobId) {
                    jobContextService.killJobs(Collections.singletonList(jobId));
                }
            },
            REAPER_INTERVAL
        );
    }

    HandlerOperations(ScheduledExecutorService executorService, final Consumer<UUID> killCursorFunction, TimeValue reaperInterval) {
        executorService.scheduleWithFixedDelay(new Reaper(killCursorFunction), 0, reaperInterval.nanos(), TimeUnit.NANOSECONDS);
    }

    @Nullable
    public ClientPagingReceiver get(UUID cursorId, TimeValue keepAlive) {
        Context context = cursorIdToReceiverMap.get(cursorId);
        if (context == null) {
            return null;
        }
        context.lastAccess = System.nanoTime();
        context.keepAlive = keepAlive;
        return context.clientPagingReceiver;
    }

    public void register(final UUID cursorId, TimeValue keepAlive, final ClientPagingReceiver clientPagingReceiver) {
        clientPagingReceiver.addListener(new CompletionListener() {
            @Override
            public void onSuccess(@Nullable CompletionState result) {
                cursorIdToReceiverMap.remove(cursorId);
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                cursorIdToReceiverMap.remove(cursorId);
            }
        });

        final Context context = new Context(clientPagingReceiver, keepAlive);
        clientPagingReceiver.resultFuture.addListener(new Runnable() {
            @Override
            public void run() {
                context.lastAccess = System.nanoTime();
            }
        }, MoreExecutors.directExecutor());
        cursorIdToReceiverMap.put(cursorId, context);
    }

    static class Context {
        private final ClientPagingReceiver clientPagingReceiver;
        private TimeValue keepAlive;

        // this is null until the first result has been produced to avoid killing long running queries like copy from
        Long lastAccess = null;

        public Context(ClientPagingReceiver clientPagingReceiver, TimeValue keepAlive) {
            this.clientPagingReceiver = clientPagingReceiver;
            this.keepAlive = keepAlive;
            this.lastAccess = System.nanoTime();
        }
    }

    private class Reaper implements Runnable {
        private final Consumer<UUID> killCursorFunction;

        Reaper(Consumer<UUID> killCursorFunction) {
            this.killCursorFunction = killCursorFunction;
        }

        @Override
        public void run() {
            long now = System.nanoTime();
            Iterator<Map.Entry<UUID, Context>> it = cursorIdToReceiverMap.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<UUID, Context> entry = it.next();
                Context context = entry.getValue();
                if (context.lastAccess == null) {
                    continue;
                }
                if (now - context.lastAccess > context.keepAlive.nanos()) {
                    it.remove();
                    killCursorFunction.accept(entry.getKey());
                }
            }
        }
    }
}
