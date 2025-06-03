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

package io.crate.execution.engine;

import io.crate.concurrent.CountdownFuture;

/**
 * Triggers {@link #future} after {@code numServer} calls to either
 * {@link #jobInitialized()} or {@link #jobInitializationFailed(Throwable)}
 * <p>
 *     If {@link #jobInitializationFailed(Throwable)}} has been called at least once
 *     the future will emit a failure.
 * </p>
 */
class InitializationTracker {

    final CountdownFuture future;

    InitializationTracker(int numServer) {
        assert numServer > 0 : "Must have at least one server";
        future = new CountdownFuture(numServer);
    }

    void jobInitialized() {
        future.onSuccess();
    }

    /**
     * Indicates that a jobInitialization failed
     *
     * @param t The cause of the initialization failure.
     *          If no failure has been set so far or if it was an InterruptedException it is overwritten.
     */
    void jobInitializationFailed(Throwable t) {
        future.onFailure(prevFailure -> {
            if (prevFailure == null || prevFailure instanceof InterruptedException) {
                return t;
            }
            return prevFailure;
        });
    }

    @Override
    public String toString() {
        return "InitializationTracker{" + future + "}";
    }
}
