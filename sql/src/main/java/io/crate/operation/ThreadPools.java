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

package io.crate.operation;

import com.google.common.collect.Iterables;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

public class ThreadPools {

    /**
     * runs each runnable of the runnableCollection in it's own thread unless there aren't enough threads available.
     * In that case it will partition the runnableCollection to match the number of available threads.
     *
     * @throws RejectedExecutionException in case all threads are busy and overloaded.
     */
    public static void runWithAvailableThreads(ThreadPoolExecutor executor,
                                               int poolSize,
                                               Collection<Runnable> runnableCollection) throws RejectedExecutionException {
        int availableThreads = Math.max(poolSize - executor.getActiveCount(), 2);
        if (availableThreads < runnableCollection.size()) {
            Iterable<List<Runnable>> partition = Iterables.partition(runnableCollection,
                    runnableCollection.size() / availableThreads);

            for (final List<Runnable> runnableList : partition) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        for (Runnable runnable : runnableList) {
                            runnable.run();
                        }
                    }
                });
            }
        } else {
            for (Runnable runnable : runnableCollection) {
                executor.execute(runnable);
            }
        }
    }
}
