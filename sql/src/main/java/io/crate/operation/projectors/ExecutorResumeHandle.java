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

package io.crate.operation.projectors;

import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

public class ExecutorResumeHandle implements ResumeHandle {

    private final Executor executor;
    private final Runnable runnable;

    public ExecutorResumeHandle(Executor executor, Runnable runnable) {
        this.executor = executor;
        this.runnable = runnable;
    }

    @Override
    public void resume(boolean async) {
        resume(executor, runnable, async);
    }

    public static void resume(Executor executor, Runnable runnable, boolean async) {
        if (async) {
            try {
                executor.execute(runnable);
            } catch (EsRejectedExecutionException | RejectedExecutionException e) {
                runnable.run();
            }
        } else {
            runnable.run();
        }
    }
}
