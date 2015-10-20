/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.concurrent;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadedStatement extends Statement {

    private final Statement statement;
    private final int count;

    public ThreadedStatement(Statement base, Description description) {
        this.statement = base;

        Threaded threaded = description.getAnnotation(Threaded.class);
        count = threaded != null ? threaded.count() : 0;

    }

    @Override
    public void evaluate() throws Throwable {
        if (count <= 0) {
            statement.evaluate();
        } else {
            final CountDownLatch startLatch = new CountDownLatch(count);
            ExecutorService executorService = Executors.newFixedThreadPool(count, EsExecutors.daemonThreadFactory(ImmutableSettings.EMPTY, "Threaded"));
            ListeningExecutorService listeningExecutorService = MoreExecutors.listeningDecorator(executorService);

            List<ListenableFuture<?>> futures = new ArrayList<>(count);
            try {
                for (int i = 0; i < count; i++) {
                    futures.add(listeningExecutorService.<Void>submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                startLatch.await(); // all threads wait for each other
                                statement.evaluate();
                            } catch (Throwable t) {
                                throw new RuntimeException(t);
                            }
                        }
                    }));

                    startLatch.countDown();
                }
                for (int i = 0; i < count; i++) {
                    try {
                        Futures.allAsList(futures).get();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (ExecutionException e) {
                        throw e.getCause();
                    }
                }
            } finally {
                executorService.shutdownNow();
            }

        }
    }
}
