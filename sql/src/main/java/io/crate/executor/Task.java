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

package io.crate.executor;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

/**
 * A task gets executed as part of or in the context of a
 * {@linkplain io.crate.executor.Job} and returns a result asynchronously
 * as a list of futures.
 * <p>
 * create a task, call {@linkplain #execute()} or {@linkplain #executeBulk()}
 * and wait for the futures returned to fetch the result or raise an exception.
 *
 * @see io.crate.executor.Job
 */
public interface Task {

    /**
     * execute the task
     */
    ListenableFuture<TaskResult> execute();

    /**
     * execute the bulk operation
     *
     * @throws UnsupportedOperationException if the task doesn't support bulk operations
     */
    List<? extends ListenableFuture<TaskResult>> executeBulk();
}
