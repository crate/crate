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

package io.crate.operation.join.nestedloop;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;

import java.util.concurrent.*;

public class NestedLoopExecutorService extends AbstractLifecycleComponent<NestedLoopExecutorService> {

    public static final String MAX_THREADS_SETTING = "nestedloop.max_threads";
    public static final int DEFAULT_MAX_THREADS = 100;

    public static final String THREAD_NAME = "io.crate.nestedloop";

    private final int maxThreads;
    private ThreadPoolExecutor nestedLoopExecutor;

    @Inject
    public NestedLoopExecutorService(Settings settings) {
        super(settings);
        this.maxThreads = settings.getAsInt(MAX_THREADS_SETTING, DEFAULT_MAX_THREADS);
    }

    public Executor executor() {
        return nestedLoopExecutor;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        nestedLoopExecutor = new ThreadPoolExecutor(
                1,
                maxThreads,
                60L,
                TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(),
                EsExecutors.daemonThreadFactory(THREAD_NAME)
        );
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        nestedLoopExecutor.shutdown();
        try {
            nestedLoopExecutor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw ExceptionsHelper.convertToElastic(e);
        } finally {
            nestedLoopExecutor.shutdownNow();
        }
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        doStop();
    }
}
