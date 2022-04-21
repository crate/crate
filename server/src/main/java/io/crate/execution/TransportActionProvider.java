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

package io.crate.execution;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;

import io.crate.cluster.decommission.TransportDecommissionNodeAction;
import io.crate.execution.engine.fetch.TransportFetchNodeAction;
import io.crate.execution.engine.profile.TransportCollectProfileNodeAction;
import io.crate.execution.jobs.kill.TransportKillAllNodeAction;
import io.crate.execution.jobs.kill.TransportKillJobsNodeAction;
import io.crate.execution.jobs.transport.TransportJobAction;
import io.crate.planner.DependencyCarrier;

/**
 * @deprecated Prefer using {@link DependencyCarrier#client()} and
 *             {@link ElasticsearchClient#execute(org.elasticsearch.action.ActionType, org.elasticsearch.transport.TransportRequest)}
 **/
@Deprecated
public class TransportActionProvider {

    private final Provider<TransportFetchNodeAction> transportFetchNodeActionProvider;
    private final Provider<TransportCollectProfileNodeAction> transportCollectProfileNodeActionProvider;
    private final Provider<TransportJobAction> transportJobInitActionProvider;
    private final Provider<TransportKillAllNodeAction> transportKillAllNodeActionProvider;
    private final Provider<TransportKillJobsNodeAction> transportKillJobsNodeActionProvider;
    private final Provider<TransportDecommissionNodeAction> transportDecommissionNodeActionProvider;

    @Inject
    public TransportActionProvider(Provider<TransportFetchNodeAction> transportFetchNodeActionProvider,
                                   Provider<TransportCollectProfileNodeAction> transportCollectProfileNodeActionProvider,
                                   Provider<TransportKillAllNodeAction> transportKillAllNodeActionProvider,
                                   Provider<TransportJobAction> transportJobInitActionProvider,
                                   Provider<TransportKillJobsNodeAction> transportKillJobsNodeActionProvider,
                                   Provider<TransportDecommissionNodeAction> transportDecommissionNodeActionProvider) {
        this.transportKillAllNodeActionProvider = transportKillAllNodeActionProvider;
        this.transportFetchNodeActionProvider = transportFetchNodeActionProvider;
        this.transportCollectProfileNodeActionProvider = transportCollectProfileNodeActionProvider;
        this.transportJobInitActionProvider = transportJobInitActionProvider;
        this.transportKillJobsNodeActionProvider = transportKillJobsNodeActionProvider;
        this.transportDecommissionNodeActionProvider = transportDecommissionNodeActionProvider;
    }

    public TransportJobAction transportJobInitAction() {
        return transportJobInitActionProvider.get();
    }

    public TransportFetchNodeAction transportFetchNodeAction() {
        return transportFetchNodeActionProvider.get();
    }

    public TransportCollectProfileNodeAction transportCollectProfileNodeAction() {
        return transportCollectProfileNodeActionProvider.get();
    }

    public TransportKillAllNodeAction transportKillAllNodeAction() {
        return transportKillAllNodeActionProvider.get();
    }

    public TransportKillJobsNodeAction transportKillJobsNodeAction() {
        return transportKillJobsNodeActionProvider.get();
    }

    public TransportDecommissionNodeAction transportDecommissionNodeAction() {
        return transportDecommissionNodeActionProvider.get();
    }
}
