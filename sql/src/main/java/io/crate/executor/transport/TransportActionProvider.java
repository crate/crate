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

package io.crate.executor.transport;

import io.crate.action.job.TransportJobAction;
import io.crate.executor.transport.kill.TransportKillAllNodeAction;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.TransportDeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.TransportRestoreSnapshotAction;
import org.elasticsearch.action.admin.indices.create.TransportBulkCreateIndicesAction;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.get.TransportMultiGetAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;

public class TransportActionProvider {

    private final Provider<TransportFetchNodeAction> transportFetchNodeActionProvider;

    private final Provider<TransportDeleteIndexAction> transportDeleteIndexActionProvider;
    private final Provider<TransportClusterUpdateSettingsAction> transportClusterUpdateSettingsActionProvider;
    private final Provider<TransportShardDeleteAction> transportShardDeleteActionProvider;
    private final Provider<TransportDeleteAction> transportDeleteActionProvider;

    private final Provider<TransportGetAction> transportGetActionProvider;
    private final Provider<TransportMultiGetAction> transportMultiGetActionProvider;
    private final Provider<TransportShardUpsertAction> transportShardUpsertActionProvider;
    private final Provider<TransportBulkCreateIndicesAction> transportBulkCreateIndicesActionProvider;

    private final Provider<TransportJobAction> transportJobInitActionProvider;
    private final Provider<TransportKillAllNodeAction> transportKillAllNodeActionProvider;
    private final Provider<TransportKillJobsNodeAction> transportKillJobsNodeActionProvider;

    private final Provider<TransportCreateSnapshotAction> transportCreateSnapshotActionProvider;
    private final Provider<TransportDeleteSnapshotAction> transportDeleteSnapshotActionProvider;
    private final Provider<TransportRestoreSnapshotAction> transportRestoreSnapshotActionProvider;
    private final Provider<TransportGetSnapshotsAction> transportGetSnapshotsActionProvider;

    @Inject
    public TransportActionProvider(Provider<TransportFetchNodeAction> transportFetchNodeActionProvider,
                                   Provider<TransportDeleteIndexAction> transportDeleteIndexActionProvider,
                                   Provider<TransportClusterUpdateSettingsAction> transportClusterUpdateSettingsActionProvider,
                                   Provider<TransportShardDeleteAction> transportShardDeleteActionProvider,
                                   Provider<TransportDeleteAction> transportDeleteActionProvider,
                                   Provider<TransportGetAction> transportGetActionProvider,
                                   Provider<TransportMultiGetAction> transportMultiGetActionProvider,
                                   Provider<TransportShardUpsertAction> transportShardUpsertActionProvider,
                                   Provider<TransportKillAllNodeAction> transportKillAllNodeActionProvider,
                                   Provider<TransportJobAction> transportJobInitActionProvider,
                                   Provider<TransportBulkCreateIndicesAction> transportBulkCreateIndicesActionProvider,
                                   Provider<TransportKillJobsNodeAction> transportKillJobsNodeActionProvider,
                                   Provider<TransportDeleteSnapshotAction> transportDeleteSnapshotActionProvider,
                                   Provider<TransportCreateSnapshotAction> transportCreateSnapshotActionProvider,
                                   Provider<TransportRestoreSnapshotAction> transportRestoreSnapshotActionProvider,
                                   Provider<TransportGetSnapshotsAction> transportGetSnapshotsActionPovider) {
        this.transportDeleteIndexActionProvider = transportDeleteIndexActionProvider;
        this.transportClusterUpdateSettingsActionProvider = transportClusterUpdateSettingsActionProvider;
        this.transportShardDeleteActionProvider = transportShardDeleteActionProvider;
        this.transportDeleteActionProvider = transportDeleteActionProvider;
        this.transportGetActionProvider = transportGetActionProvider;
        this.transportMultiGetActionProvider = transportMultiGetActionProvider;
        this.transportShardUpsertActionProvider = transportShardUpsertActionProvider;
        this.transportKillAllNodeActionProvider = transportKillAllNodeActionProvider;
        this.transportFetchNodeActionProvider = transportFetchNodeActionProvider;
        this.transportJobInitActionProvider = transportJobInitActionProvider;
        this.transportBulkCreateIndicesActionProvider = transportBulkCreateIndicesActionProvider;
        this.transportKillJobsNodeActionProvider = transportKillJobsNodeActionProvider;
        this.transportDeleteSnapshotActionProvider = transportDeleteSnapshotActionProvider;
        this.transportCreateSnapshotActionProvider = transportCreateSnapshotActionProvider;
        this.transportRestoreSnapshotActionProvider = transportRestoreSnapshotActionProvider;
        this.transportGetSnapshotsActionProvider = transportGetSnapshotsActionPovider;
    }

    public TransportBulkCreateIndicesAction transportBulkCreateIndicesAction() {
        return transportBulkCreateIndicesActionProvider.get();
    }

    public TransportDeleteIndexAction transportDeleteIndexAction() {
        return transportDeleteIndexActionProvider.get();
    }

    public TransportClusterUpdateSettingsAction transportClusterUpdateSettingsAction() {
        return transportClusterUpdateSettingsActionProvider.get();
    }

    public TransportDeleteAction transportDeleteAction() {
        return transportDeleteActionProvider.get();
    }

    public TransportGetAction transportGetAction() {
        return transportGetActionProvider.get();
    }

    public TransportMultiGetAction transportMultiGetAction() {
        return transportMultiGetActionProvider.get();
    }

    public TransportShardUpsertAction transportShardUpsertAction() {
        return transportShardUpsertActionProvider.get();
    }

    public TransportShardDeleteAction transportShardDeleteAction() {
        return transportShardDeleteActionProvider.get();
    }

    public TransportJobAction transportJobInitAction() {
        return transportJobInitActionProvider.get();
    }

    public TransportFetchNodeAction transportFetchNodeAction() {
        return transportFetchNodeActionProvider.get();
    }

    public TransportKillAllNodeAction transportKillAllNodeAction() {
        return transportKillAllNodeActionProvider.get();
    }

    public TransportKillJobsNodeAction transportKillJobsNodeAction() {
        return transportKillJobsNodeActionProvider.get();
    }

    public TransportDeleteSnapshotAction transportDeleteSnapshotAction() {
        return transportDeleteSnapshotActionProvider.get();
    }

    public TransportCreateSnapshotAction transportCreateSnapshotAction() {
        return transportCreateSnapshotActionProvider.get();
    }

    public TransportRestoreSnapshotAction transportRestoreSnapshotAction() {
        return transportRestoreSnapshotActionProvider.get();
    }

    public TransportGetSnapshotsAction transportGetSnapshotsAction() {
        return transportGetSnapshotsActionProvider.get();
    }

}
