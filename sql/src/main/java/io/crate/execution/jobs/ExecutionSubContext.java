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

package io.crate.execution.jobs;

import io.crate.concurrent.CompletionListenable;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

public interface ExecutionSubContext extends CompletionListenable {


    /**
     * In the prepare phase implementations of this interface can allocate any resources.
     * Exception are required to be thrown directly and must not be set on the downstream.
     */
    void prepare() throws Exception;

    /**
     * In the start phase implementations of this interface are required to start any executors.
     * <p>
     * In this phase failures must not be propagated to downstream phases directly.
     * <p>
     * However, it is ok for the started executors to use their downstreams to propagate failures.
     */
    void start();

    void kill(@Nullable Throwable throwable);

    String name();

    int id();

    /**
     * Hook to cleanup the resources of this context. This might be called at any time in the lifecycle of the context.
     */
    void cleanup();

    @Override
    CompletableFuture<CompletionState> completionFuture();
}
