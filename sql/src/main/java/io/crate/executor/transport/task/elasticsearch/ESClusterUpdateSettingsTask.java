/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.executor.transport.task.elasticsearch;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.TaskResult;
import io.crate.executor.JobTask;
import io.crate.planner.node.ddl.ESClusterUpdateSettingsNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

public class ESClusterUpdateSettingsTask extends JobTask {

    private final List<ListenableFuture<TaskResult>> results;
    private final TransportClusterUpdateSettingsAction transport;
    private final ClusterUpdateSettingsRequest request;
    private final ActionListener<ClusterUpdateSettingsResponse> listener;

    public ESClusterUpdateSettingsTask(UUID jobId,
                                       TransportClusterUpdateSettingsAction transport,
                                       ESClusterUpdateSettingsNode node) {
        super(jobId);
        this.transport = transport;

        final SettableFuture<TaskResult> result = SettableFuture.create();
        results = Arrays.<ListenableFuture<TaskResult>>asList(result);

        request = new ClusterUpdateSettingsRequest();
        request.persistentSettings(node.persistentSettings());
        request.transientSettings(node.transientSettings());
        if (node.persistentSettingsToRemove() != null) {
            request.persistentSettingsToRemove(node.persistentSettingsToRemove());
        }
        if (node.transientSettingsToRemove() != null) {
            request.transientSettingsToRemove(node.transientSettingsToRemove());
        }
        listener = new ClusterUpdateSettingsResponseListener(result);
    }

    static class ClusterUpdateSettingsResponseListener implements ActionListener<ClusterUpdateSettingsResponse> {

        private final SettableFuture<TaskResult> result;

        public ClusterUpdateSettingsResponseListener(SettableFuture<TaskResult> result) {
            this.result = result;
        }

        @Override
        public void onResponse(ClusterUpdateSettingsResponse response) {
            result.set(TaskResult.ONE_ROW);
        }

        @Override
        public void onFailure(Throwable e) {
            result.setException(e);
        }
    }

    @Override
    public void start() {
        transport.execute(request, listener);
    }

    @Override
    public List<ListenableFuture<TaskResult>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
        throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "upstreamResult not supported on %s",
                        getClass().getSimpleName()));
    }
}
