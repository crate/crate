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

package org.elasticsearch.cluster.graceful;

import com.google.common.base.Predicate;
import com.google.common.util.concurrent.ListenableFuture;
import org.elasticsearch.cluster.routing.MutableShardRouting;

import javax.annotation.Nullable;

/**
 * deallocates shards on one node, moving them to other nodes
 */
public interface Deallocator {

    /**
     * TODO: refine
     */
    public static class DeallocationResult {

        public static final DeallocationResult SUCCESS_NOTHING_HAPPENED = new DeallocationResult(true, false);
        public static final DeallocationResult SUCCESS = new DeallocationResult(true, true);
        private final boolean success;
        private final boolean didDeallocate;

        protected DeallocationResult(boolean success, boolean didDeallocate) {
            this.success = success;
            this.didDeallocate = didDeallocate;
        }


        public boolean success() {
            return success;
        }


        public boolean didDeallocate() {
            return didDeallocate;
        }
    }

    static final Predicate<MutableShardRouting> ALL_SHARDS = new Predicate<MutableShardRouting>() {
        @Override
        public boolean apply(@Nullable MutableShardRouting input) {
            return input != null
                    && (input.started() || input.initializing());
        }
    };

    static final Predicate<MutableShardRouting> ALL_PRIMARY_SHARDS = new Predicate<MutableShardRouting>() {
        @Override
        public boolean apply(@Nullable MutableShardRouting input) {
            return input != null
                    && (input.started() || input.initializing())
                    && input.primary();
        }
    };

    /**
     * asynchronously deallocate shard of the local node
     *
     */
    public ListenableFuture<DeallocationResult> deallocate();

    /**
     * cancel a currently running deallocation
     *
     * this is not similar to a rollback, if some shards have already been
     * moved they will not be moved back by this method.
     *
     * @return true if a running deallocation was stopped
     */
    public boolean cancel();

    public boolean isDeallocating();
}
