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

package io.crate.executor.transport.executionphases;

import io.crate.action.job.JobResponse;
import io.crate.data.BatchConsumer;
import io.crate.planner.node.ExecutionPhase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.Tuple;

import java.util.List;

class FailureOnlyResponseListener implements ActionListener<JobResponse> {

    private final List<Tuple<ExecutionPhase, BatchConsumer>> consumers;
    private final InitializationTracker initializationTracker;

    FailureOnlyResponseListener(List<Tuple<ExecutionPhase, BatchConsumer>> consumers, InitializationTracker initializationTracker) {
        this.consumers = consumers;
        this.initializationTracker = initializationTracker;
    }

    @Override
    public void onResponse(JobResponse jobResponse) {
        initializationTracker.jobInitialized();
        if (jobResponse.directResponse().size() > 0) {
            for (Tuple<ExecutionPhase, BatchConsumer> consumer : consumers) {
                consumer.v2().accept(null, new IllegalStateException("Got a directResponse but didn't expect one"));
            }
        }
    }

    @Override
    public void onFailure(Throwable e) {
        initializationTracker.jobInitialized();
        // could be a preparation failure - in that case the regular error propagation doesn't work as it hasn't been set up yet
        // so fail rowReceivers directly
        for (Tuple<ExecutionPhase, BatchConsumer> consumer : consumers) {
            consumer.v2().accept(null, e);
        }
    }
}
