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

package io.crate.execution.jobs;

import java.util.concurrent.CompletableFuture;

import org.jspecify.annotations.Nullable;

import io.crate.common.concurrent.Killable;
import io.crate.concurrent.CompletionListenable;

public interface Task extends CompletionListenable<Void>, Killable {

    /**
     * Start a task.
     * @return future that is completed once the start action completed its preparation phase.
     *         This does *NOT* indicate that the task finished. Use {@link #completionFuture()} for that.
     *         If null the start method is synchronous and users can assume the task is initiated once
     *         the method returns.
     */
    @Nullable
    CompletableFuture<Void> start();

    String name();

    int id();

    long bytesUsed();
}
