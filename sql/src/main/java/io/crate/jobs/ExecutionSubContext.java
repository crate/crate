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

package io.crate.jobs;

import com.google.common.base.Predicate;

import javax.annotation.Nullable;

public interface ExecutionSubContext {

    public static final Predicate<ExecutionSubContext> IS_ACTIVE_PREDICATE = new Predicate<ExecutionSubContext>() {
        @Override
        public boolean apply(@Nullable ExecutionSubContext input) {
            return input != null && input.subContextMode() == SubContextMode.ACTIVE;
        }
    };

    /**
     * enum for the current mode of a ExecutionSubContext.
     * Might be static or changing dynamically
     * (e.g. becoming active when a data was received and is currently consumed).
     */
    public static enum SubContextMode {
        /**
         * an active subcontext is active itself,
         * keeping its execution context alive.
         */
        ACTIVE,
        /**
         * a passive subcontext usually idles waiting for data.
         * it might need external keep alives in order to not be shut down.
         */
        PASSIVE
    }

    /**
     * Set the {@link KeepAliveListener} on this subcontext. This is required to be set before
     * any of the other methods are called.
     */
    void keepAliveListener(KeepAliveListener listener);

    /**
     * In the prepare phase implementations of this interface can allocate any resources.
     *
     * In this phase failures must not be propagated to downstream phases directly, but instead are required to be set on the
     * {@link ExecutionSubContext#future()}.
     */
    void prepare();

    /**
     * In the start phase implementations of this interface are required to start any executors.
     *
     * In this phase failures must not be propagated to downstream phases directly, but instead are required to be
     * set on the {@link ExecutionSubContext#future()}.
     *
     * However, it is ok for the started executors to use their downstreams to propagate failures.
     */
    void start();

    void close();

    void kill(@Nullable Throwable throwable);

    String name();

    SubExecutionContextFuture future();

    int id();

    /**
     * get the current state in respect to keep-alive handling
     */
    SubContextMode subContextMode();
}
