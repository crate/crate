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
import org.elasticsearch.common.Nullable;

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

    void addCallback(ContextCallback contextCallback);
    void start();
    void close();
    void kill();

    String name();

    /**
     * get the current state in respect to keep-alive handling
     */
    SubContextMode subContextMode();
}
