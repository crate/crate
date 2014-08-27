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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;

import java.util.Locale;

public class Deallocators {

    public static final String GRACEFUL_STOP_MIN_AVAILABILITY = "cluster.graceful_stop.min_availability";

    private final AllShardsDeallocator allShardsDeallocator;
    private final PrimariesDeallocator primariesDeallocator;
    private final ClusterService clusterService;

    private Deallocator noOpDeallocator = new Deallocator() {
        @Override
        public ListenableFuture<DeallocationResult> deallocate() {
            return Futures.immediateFuture(DeallocationResult.SUCCESS_NOTHING_HAPPENED);
        }

        @Override
        public boolean cancel() {
            return false;
        }

        @Override
        public boolean isDeallocating() {
            return false;
        }
    };

    @Inject
    public Deallocators(ClusterService clusterService, AllShardsDeallocator allShardsDeallocator, PrimariesDeallocator primariesDeallocator) {
        this.clusterService = clusterService;
        this.allShardsDeallocator = allShardsDeallocator;
        this.primariesDeallocator = primariesDeallocator;
    }

    public ListenableFuture<Deallocator.DeallocationResult> deallocate() {
        return getDeallocator().deallocate();
    }

    private Deallocator getDeallocator() {
        Deallocator deallocator;
        String minAvailability = clusterService.state().metaData().settings().get(GRACEFUL_STOP_MIN_AVAILABILITY);
        switch (minAvailability) {
            case "primaries":
                deallocator = primariesDeallocator;
                break;
            case "full":
                deallocator = allShardsDeallocator;
                break;
            case "none":
                deallocator = noOpDeallocator;
                break;
            default:
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "invalid setting for '%s'", GRACEFUL_STOP_MIN_AVAILABILITY));
        }
        return deallocator;
    }
}
