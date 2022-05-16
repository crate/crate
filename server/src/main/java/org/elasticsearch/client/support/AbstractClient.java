/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.support;

import java.util.concurrent.CompletableFuture;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequestBuilder;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequestBuilder;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksAction;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequestBuilder;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequestBuilder;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeAction;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;

public abstract class AbstractClient implements Client {

    private final ThreadPool threadPool;
    private final Admin admin;
    protected final Settings settings;

    public AbstractClient(Settings settings, ThreadPool threadPool) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.admin = new Admin(this);
    }

    @Override
    public final Settings settings() {
        return this.settings;
    }

    @Override
    public final ThreadPool threadPool() {
        return this.threadPool;
    }

    @Override
    public final AdminClient admin() {
        return admin;
    }

    /**
     * This is the single execution point of *all* clients.
     */
    @Override
    public final <Request extends TransportRequest, Response extends TransportResponse> void execute(ActionType<Response> action,
                                                                                                     Request request,
                                                                                                     ActionListener<Response> listener) {
        doExecute(action, request, listener);
    }

    protected abstract <Request extends TransportRequest, Response extends TransportResponse> void doExecute(ActionType<Response> action,
                                                                                                             Request request,
                                                                                                             ActionListener<Response> listener);

    static class Admin implements AdminClient {

        private final ClusterAdmin clusterAdmin;
        private final IndicesAdmin indicesAdmin;

        Admin(ElasticsearchClient client) {
            this.clusterAdmin = new ClusterAdmin(client);
            this.indicesAdmin = new IndicesAdmin(client);
        }

        @Override
        public ClusterAdminClient cluster() {
            return clusterAdmin;
        }

        @Override
        public IndicesAdminClient indices() {
            return indicesAdmin;
        }
    }

    static class ClusterAdmin implements ClusterAdminClient {

        private final ElasticsearchClient client;

        ClusterAdmin(ElasticsearchClient client) {
            this.client = client;
        }

        @Override
        public <Request extends TransportRequest, Response extends TransportResponse> void execute(ActionType<Response> action,
                                                                                                   Request request,
                                                                                                   ActionListener<Response> listener) {
            client.execute(action, request).whenComplete(ActionListener.toBiConsumer(listener));
        }

        @Override
        public ThreadPool threadPool() {
            return client.threadPool();
        }

        @Override
        public ActionFuture<ClusterHealthResponse> health(final ClusterHealthRequest request) {
            return legacyExecute(ClusterHealthAction.INSTANCE, request);
        }

        @Override
        public ClusterHealthRequestBuilder prepareHealth(String... indices) {
            return new ClusterHealthRequestBuilder(this, ClusterHealthAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<ClusterStateResponse> state(final ClusterStateRequest request) {
            return legacyExecute(ClusterStateAction.INSTANCE, request);
        }

        @Override
        public ClusterStateRequestBuilder prepareState() {
            return new ClusterStateRequestBuilder(this, ClusterStateAction.INSTANCE);
        }

        @Override
        public ClusterRerouteRequestBuilder prepareReroute() {
            return new ClusterRerouteRequestBuilder(this, ClusterRerouteAction.INSTANCE);
        }

        @Override
        public ClusterUpdateSettingsRequestBuilder prepareUpdateSettings() {
            return new ClusterUpdateSettingsRequestBuilder(this, ClusterUpdateSettingsAction.INSTANCE);
        }

        @Override
        public PendingClusterTasksRequestBuilder preparePendingClusterTasks() {
            return new PendingClusterTasksRequestBuilder(this, PendingClusterTasksAction.INSTANCE);
        }

        @Override
        public PutRepositoryRequestBuilder preparePutRepository(String name) {
            return new PutRepositoryRequestBuilder(this, PutRepositoryAction.INSTANCE, name);
        }

        @Override
        public CreateSnapshotRequestBuilder prepareCreateSnapshot(String repository, String name) {
            return new CreateSnapshotRequestBuilder(this, CreateSnapshotAction.INSTANCE, repository, name);
        }

        @Override
        public GetSnapshotsRequestBuilder prepareGetSnapshots(String repository) {
            return new GetSnapshotsRequestBuilder(this, GetSnapshotsAction.INSTANCE, repository);
        }

        @Override
        public void nodesStats(final NodesStatsRequest request, final ActionListener<NodesStatsResponse> listener) {
            execute(NodesStatsAction.INSTANCE, request).whenComplete(ActionListener.toBiConsumer(listener));
        }
    }

    static class IndicesAdmin implements IndicesAdminClient {

        private final ElasticsearchClient client;

        IndicesAdmin(ElasticsearchClient client) {
            this.client = client;
        }

        @Override
        public <Request extends TransportRequest, Response extends TransportResponse> void execute(ActionType<Response> action,
                                                                                                   Request request,
                                                                                                   ActionListener<Response> listener) {
            client.execute(action, request).whenComplete(ActionListener.toBiConsumer(listener));
        }

        @Override
        public ThreadPool threadPool() {
            return client.threadPool();
        }

        @Override
        public ActionFuture<CreateIndexResponse> create(final CreateIndexRequest request) {
            return legacyExecute(CreateIndexAction.INSTANCE, request);
        }

        @Override
        public CreateIndexRequestBuilder prepareCreate(String index) {
            return new CreateIndexRequestBuilder(this, CreateIndexAction.INSTANCE, index);
        }

        @Override
        public CompletableFuture<AcknowledgedResponse> delete(final DeleteIndexRequest request) {
            return execute(DeleteIndexAction.INSTANCE, request);
        }

        @Override
        public ActionFuture<AcknowledgedResponse> putMapping(final PutMappingRequest request) {
            return legacyExecute(PutMappingAction.INSTANCE, request);
        }

        @Override
        public PutMappingRequestBuilder preparePutMapping(String... indices) {
            return new PutMappingRequestBuilder(this, PutMappingAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<UpgradeResponse> upgrade(final UpgradeRequest request) {
            return legacyExecute(UpgradeAction.INSTANCE, request);
        }

        @Override
        public CompletableFuture<RefreshResponse> refresh(final RefreshRequest request) {
            return execute(RefreshAction.INSTANCE, request);
        }

        @Override
        public ActionFuture<IndicesStatsResponse> stats(final IndicesStatsRequest request) {
            return legacyExecute(IndicesStatsAction.INSTANCE, request);
        }

        @Override
        public void stats(final IndicesStatsRequest request, final ActionListener<IndicesStatsResponse> listener) {
            execute(IndicesStatsAction.INSTANCE, request).whenComplete(ActionListener.toBiConsumer(listener));
        }

        @Override
        public IndicesStatsRequestBuilder prepareStats(String... indices) {
            return new IndicesStatsRequestBuilder(this, IndicesStatsAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<RecoveryResponse> recoveries(final RecoveryRequest request) {
            return legacyExecute(RecoveryAction.INSTANCE, request);
        }


        @Override
        public ActionFuture<AcknowledgedResponse> updateSettings(final UpdateSettingsRequest request) {
            return legacyExecute(UpdateSettingsAction.INSTANCE, request);
        }

        @Override
        public UpdateSettingsRequestBuilder prepareUpdateSettings(String... indices) {
            return new UpdateSettingsRequestBuilder(this, UpdateSettingsAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<AcknowledgedResponse> putTemplate(final PutIndexTemplateRequest request) {
            return legacyExecute(PutIndexTemplateAction.INSTANCE, request);
        }

        @Override
        public void putTemplate(final PutIndexTemplateRequest request, final ActionListener<AcknowledgedResponse> listener) {
            execute(PutIndexTemplateAction.INSTANCE, request).whenComplete(ActionListener.toBiConsumer(listener));
        }

        @Override
        public PutIndexTemplateRequestBuilder preparePutTemplate(String name) {
            return new PutIndexTemplateRequestBuilder(this, PutIndexTemplateAction.INSTANCE, name);
        }

        @Override
        public ActionFuture<GetIndexTemplatesResponse> getTemplates(final GetIndexTemplatesRequest request) {
            return legacyExecute(GetIndexTemplatesAction.INSTANCE, request);
        }

        @Override
        public GetIndexTemplatesRequestBuilder prepareGetTemplates(String... names) {
            return new GetIndexTemplatesRequestBuilder(this, GetIndexTemplatesAction.INSTANCE, names);
        }

        @Override
        public void deleteTemplate(final DeleteIndexTemplateRequest request, final ActionListener<AcknowledgedResponse> listener) {
            execute(DeleteIndexTemplateAction.INSTANCE, request).whenComplete(ActionListener.toBiConsumer(listener));
        }

        @Override
        public DeleteIndexTemplateRequestBuilder prepareDeleteTemplate(String name) {
            return new DeleteIndexTemplateRequestBuilder(this, DeleteIndexTemplateAction.INSTANCE, name);
        }
    }
}
