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

package org.elasticsearch.threadpool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.util.concurrent.EsAbortPolicy;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.PrioritizedRunnable;
import org.elasticsearch.common.util.concurrent.PriorityRunnable;
import org.elasticsearch.common.util.concurrent.SizeBlockingQueue;
import org.elasticsearch.node.Node;

/**
 * Like {@link FixedExecutorBuilder} but uses a PriorityQueue. If scheduling
 * {@link Runnable} that are not {@link PrioritizedRunnable} it wraps them into
 * a {@link PriorityRunnable} with priority normal.
 *
 * <p>
 * Doesn't use {@link PrioritizedEsThreadPoolExecutor} but a simpler variant of
 * {@link EsThreadPoolExecutor} because timeout handling and strict tie breaking
 * is not needed.
 * </p>
 **/
public final class PrioFixedExecutorBuilder extends FixedExecutorBuilder {

    /**
     * Construct a fixed executor builder.
     *
     * @param settings  the node-level settings
     * @param name      the name of the executor
     * @param size      the fixed number of threads
     * @param queueSize the size of the backing queue
     */
    public PrioFixedExecutorBuilder(Settings settings, String name, int size, int queueSize) {
        super(settings, name, size, queueSize);
    }


    @Override
    ThreadPool.ExecutorHolder build(final Settings settings) {
        final String nodeName = Node.NODE_NAME_SETTING.get(settings);
        final int size = sizeSetting.get(settings);
        final int queueSize = queueSizeSetting.get(settings);
        final ThreadFactory threadFactory = EsExecutors.daemonThreadFactory(EsExecutors.threadName(nodeName, name()));
        ExecutorService executor = new EsThreadPoolExecutor(
            nodeName + "/" + name(),
            size,
            size,
            0,
            TimeUnit.MILLISECONDS,
            new SizeBlockingQueue<>(new PriorityBlockingQueue<>(queueSize), queueSize),
            threadFactory,
            new EsAbortPolicy()
        ) {
            @Override
            protected Runnable wrapRunnable(Runnable command) {
                if (command instanceof PrioritizedRunnable) {
                    return command;
                }
                return PriorityRunnable.of(Priority.NORMAL, "implicit", command);
            }
        };
        final ThreadPool.Info info = new ThreadPool.Info(
            name(),
            ThreadPool.ThreadPoolType.FIXED,
            size,
            size,
            null,
            new SizeValue(queueSize)
        );
        return new ThreadPool.ExecutorHolder(executor, info);
    }
}
