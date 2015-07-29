/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.MoreExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import javax.annotation.Nonnull;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

public class RejectionAwareExecutor implements Executor {

    private final Executor executor;
    private final FutureCallback<?> callback;

    /**
     * returns an Executor that will either execute the command given the Executor delegate or
     * call the callback.onFailure if it receives a (Es)RejectedExecutionException
     */
    public static Executor wrapExecutor(Executor delegate, FutureCallback<?> callback) {
        if (delegate == MoreExecutors.directExecutor()) {
            // directExecutor won't reject anything...
            return delegate;
        }

        return new RejectionAwareExecutor(delegate, callback);
    }

    private RejectionAwareExecutor(Executor executorDelegate, FutureCallback<?> callback) {
        this.executor = executorDelegate;
        this.callback = callback;
    }

    @Override
    public void execute(@Nonnull Runnable command) {
        try {
            executor.execute(command);
        } catch (EsRejectedExecutionException | RejectedExecutionException e) {
            callback.onFailure(e);
        }
    }
}
