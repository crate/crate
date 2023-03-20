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

import java.util.List;

import org.elasticsearch.action.ActionListener;

import io.crate.execution.engine.JobLauncher.HandlerPhase;
import io.crate.execution.jobs.transport.JobResponse;

class FailureOnlyResponseListener implements ActionListener<JobResponse> {

    private final List<HandlerPhase> handlerPhases;
    private final InitializationTracker initializationTracker;

    FailureOnlyResponseListener(List<HandlerPhase> handlerPhases, InitializationTracker initializationTracker) {
        this.handlerPhases = handlerPhases;
        this.initializationTracker = initializationTracker;
    }

    @Override
    public void onResponse(JobResponse jobResponse) {
        initializationTracker.jobInitialized();
        if (jobResponse.hasDirectResponses()) {
            for (var handlerPhase : handlerPhases) {
                handlerPhase.consumer().accept(null, new IllegalStateException("Got a directResponse but didn't expect one"));
            }
        }
    }

    @Override
    public void onFailure(Exception e) {
        initializationTracker.jobInitializationFailed(e);
        // could be a preparation failure - in that case the regular error propagation doesn't work as it hasn't been set up yet
        // so fail rowReceivers directly
        for (var handlerPhase : handlerPhases) {
            handlerPhase.consumer().accept(null, e);
        }
    }
}
