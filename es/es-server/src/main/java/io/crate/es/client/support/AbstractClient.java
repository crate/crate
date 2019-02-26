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

package io.crate.es.client.support;

import io.crate.es.action.Action;
import io.crate.es.action.ActionFuture;
import io.crate.es.action.ActionListener;
import io.crate.es.action.ActionRequest;
import io.crate.es.action.ActionRequestBuilder;
import io.crate.es.action.ActionResponse;
import io.crate.es.action.admin.cluster.health.ClusterHealthAction;
import io.crate.es.action.admin.cluster.health.ClusterHealthRequest;
import io.crate.es.action.admin.cluster.health.ClusterHealthRequestBuilder;
import io.crate.es.action.admin.cluster.health.ClusterHealthResponse;
import io.crate.es.action.admin.cluster.repositories.delete.DeleteRepositoryAction;
import io.crate.es.action.admin.cluster.repositories.delete.DeleteRepositoryRequestBuilder;
import io.crate.es.action.admin.cluster.repositories.put.PutRepositoryAction;
import io.crate.es.action.admin.cluster.repositories.put.PutRepositoryRequestBuilder;
import io.crate.es.action.admin.cluster.reroute.ClusterRerouteAction;
import io.crate.es.action.admin.cluster.reroute.ClusterRerouteRequestBuilder;
import io.crate.es.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import io.crate.es.action.admin.cluster.settings.ClusterUpdateSettingsRequestBuilder;
import io.crate.es.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import io.crate.es.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import io.crate.es.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import io.crate.es.action.admin.cluster.snapshots.delete.DeleteSnapshotRequestBuilder;
import io.crate.es.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import io.crate.es.action.admin.cluster.snapshots.get.GetSnapshotsRequestBuilder;
import io.crate.es.action.admin.cluster.state.ClusterStateAction;
import io.crate.es.action.admin.cluster.state.ClusterStateRequest;
import io.crate.es.action.admin.cluster.state.ClusterStateRequestBuilder;
import io.crate.es.action.admin.cluster.state.ClusterStateResponse;
import io.crate.es.action.admin.cluster.tasks.PendingClusterTasksAction;
import io.crate.es.action.admin.cluster.tasks.PendingClusterTasksRequestBuilder;
import io.crate.es.action.admin.indices.create.CreateIndexAction;
import io.crate.es.action.admin.indices.create.CreateIndexRequest;
import io.crate.es.action.admin.indices.create.CreateIndexRequestBuilder;
import io.crate.es.action.admin.indices.create.CreateIndexResponse;
import io.crate.es.action.admin.indices.delete.DeleteIndexAction;
import io.crate.es.action.admin.indices.delete.DeleteIndexRequest;
import io.crate.es.action.admin.indices.delete.DeleteIndexRequestBuilder;
import io.crate.es.action.admin.indices.flush.FlushAction;
import io.crate.es.action.admin.indices.flush.FlushRequest;
import io.crate.es.action.admin.indices.flush.FlushRequestBuilder;
import io.crate.es.action.admin.indices.flush.FlushResponse;
import io.crate.es.action.admin.indices.flush.SyncedFlushAction;
import io.crate.es.action.admin.indices.flush.SyncedFlushRequest;
import io.crate.es.action.admin.indices.flush.SyncedFlushResponse;
import io.crate.es.action.admin.indices.forcemerge.ForceMergeAction;
import io.crate.es.action.admin.indices.forcemerge.ForceMergeRequest;
import io.crate.es.action.admin.indices.forcemerge.ForceMergeRequestBuilder;
import io.crate.es.action.admin.indices.forcemerge.ForceMergeResponse;
import io.crate.es.action.admin.indices.mapping.put.PutMappingAction;
import io.crate.es.action.admin.indices.mapping.put.PutMappingRequest;
import io.crate.es.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import io.crate.es.action.admin.indices.recovery.RecoveryAction;
import io.crate.es.action.admin.indices.recovery.RecoveryRequest;
import io.crate.es.action.admin.indices.recovery.RecoveryResponse;
import io.crate.es.action.admin.indices.refresh.RefreshAction;
import io.crate.es.action.admin.indices.refresh.RefreshRequest;
import io.crate.es.action.admin.indices.refresh.RefreshRequestBuilder;
import io.crate.es.action.admin.indices.refresh.RefreshResponse;
import io.crate.es.action.admin.indices.settings.get.GetSettingsAction;
import io.crate.es.action.admin.indices.settings.get.GetSettingsRequest;
import io.crate.es.action.admin.indices.settings.get.GetSettingsResponse;
import io.crate.es.action.admin.indices.settings.put.UpdateSettingsAction;
import io.crate.es.action.admin.indices.settings.put.UpdateSettingsRequest;
import io.crate.es.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import io.crate.es.action.admin.indices.stats.IndicesStatsAction;
import io.crate.es.action.admin.indices.stats.IndicesStatsRequest;
import io.crate.es.action.admin.indices.stats.IndicesStatsRequestBuilder;
import io.crate.es.action.admin.indices.stats.IndicesStatsResponse;
import io.crate.es.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import io.crate.es.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import io.crate.es.action.admin.indices.template.delete.DeleteIndexTemplateRequestBuilder;
import io.crate.es.action.admin.indices.template.get.GetIndexTemplatesAction;
import io.crate.es.action.admin.indices.template.get.GetIndexTemplatesRequest;
import io.crate.es.action.admin.indices.template.get.GetIndexTemplatesRequestBuilder;
import io.crate.es.action.admin.indices.template.get.GetIndexTemplatesResponse;
import io.crate.es.action.admin.indices.template.put.PutIndexTemplateAction;
import io.crate.es.action.admin.indices.template.put.PutIndexTemplateRequest;
import io.crate.es.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import io.crate.es.action.admin.indices.upgrade.post.UpgradeAction;
import io.crate.es.action.admin.indices.upgrade.post.UpgradeRequest;
import io.crate.es.action.admin.indices.upgrade.post.UpgradeResponse;
import io.crate.es.action.support.PlainActionFuture;
import io.crate.es.action.support.master.AcknowledgedResponse;
import io.crate.es.client.AdminClient;
import io.crate.es.client.Client;
import io.crate.es.client.ClusterAdminClient;
import io.crate.es.client.ElasticsearchClient;
import io.crate.es.client.IndicesAdminClient;
import io.crate.es.common.component.AbstractComponent;
import io.crate.es.common.settings.Settings;
import io.crate.es.threadpool.ThreadPool;

public abstract class AbstractClient extends AbstractComponent implements Client {

    private final ThreadPool threadPool;
    private final Admin admin;

    public AbstractClient(Settings settings, ThreadPool threadPool) {
        super(settings);
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

    @Override
    public final <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> ActionFuture<Response> execute(
            Action<Request, Response, RequestBuilder> action, Request request) {
        PlainActionFuture<Response> actionFuture = PlainActionFuture.newFuture();
        execute(action, request, actionFuture);
        return actionFuture;
    }

    /**
     * This is the single execution point of *all* clients.
     */
    @Override
    public final <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void execute(
            Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        doExecute(action, request, listener);
    }

    protected abstract <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener);

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
        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> ActionFuture<Response> execute(
                Action<Request, Response, RequestBuilder> action, Request request) {
            return client.execute(action, request);
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void execute(
                Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
            client.execute(action, request, listener);
        }

        @Override
        public ThreadPool threadPool() {
            return client.threadPool();
        }

        @Override
        public ActionFuture<ClusterHealthResponse> health(final ClusterHealthRequest request) {
            return execute(ClusterHealthAction.INSTANCE, request);
        }

        @Override
        public ClusterHealthRequestBuilder prepareHealth(String... indices) {
            return new ClusterHealthRequestBuilder(this, ClusterHealthAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<ClusterStateResponse> state(final ClusterStateRequest request) {
            return execute(ClusterStateAction.INSTANCE, request);
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
        public DeleteSnapshotRequestBuilder prepareDeleteSnapshot(String repository, String name) {
            return new DeleteSnapshotRequestBuilder(this, DeleteSnapshotAction.INSTANCE, repository, name);
        }

        @Override
        public DeleteRepositoryRequestBuilder prepareDeleteRepository(String name) {
            return new DeleteRepositoryRequestBuilder(this, DeleteRepositoryAction.INSTANCE, name);
        }
    }

    static class IndicesAdmin implements IndicesAdminClient {

        private final ElasticsearchClient client;

        IndicesAdmin(ElasticsearchClient client) {
            this.client = client;
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> ActionFuture<Response> execute(
                Action<Request, Response, RequestBuilder> action, Request request) {
            return client.execute(action, request);
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void execute(
                Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
            client.execute(action, request, listener);
        }

        @Override
        public ThreadPool threadPool() {
            return client.threadPool();
        }

        @Override
        public ActionFuture<CreateIndexResponse> create(final CreateIndexRequest request) {
            return execute(CreateIndexAction.INSTANCE, request);
        }

        @Override
        public void create(final CreateIndexRequest request, final ActionListener<CreateIndexResponse> listener) {
            execute(CreateIndexAction.INSTANCE, request, listener);
        }

        @Override
        public CreateIndexRequestBuilder prepareCreate(String index) {
            return new CreateIndexRequestBuilder(this, CreateIndexAction.INSTANCE, index);
        }

        @Override
        public ActionFuture<AcknowledgedResponse> delete(final DeleteIndexRequest request) {
            return execute(DeleteIndexAction.INSTANCE, request);
        }

        @Override
        public void delete(final DeleteIndexRequest request, final ActionListener<AcknowledgedResponse> listener) {
            execute(DeleteIndexAction.INSTANCE, request, listener);
        }

        @Override
        public DeleteIndexRequestBuilder prepareDelete(String... indices) {
            return new DeleteIndexRequestBuilder(this, DeleteIndexAction.INSTANCE, indices);
        }

        @Override
        public ActionFuture<FlushResponse> flush(final FlushRequest request) {
            return execute(FlushAction.INSTANCE, request);
        }

        @Override
        public void flush(final FlushRequest request, final ActionListener<FlushResponse> listener) {
            execute(FlushAction.INSTANCE, request, listener);
        }

        @Override
        public FlushRequestBuilder prepareFlush(String... indices) {
            return new FlushRequestBuilder(this, FlushAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<SyncedFlushResponse> syncedFlush(SyncedFlushRequest request) {
            return execute(SyncedFlushAction.INSTANCE, request);
        }

        @Override
        public void syncedFlush(SyncedFlushRequest request, ActionListener<SyncedFlushResponse> listener) {
            execute(SyncedFlushAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<AcknowledgedResponse> putMapping(final PutMappingRequest request) {
            return execute(PutMappingAction.INSTANCE, request);
        }

        @Override
        public void putMapping(final PutMappingRequest request, final ActionListener<AcknowledgedResponse> listener) {
            execute(PutMappingAction.INSTANCE, request, listener);
        }

        @Override
        public PutMappingRequestBuilder preparePutMapping(String... indices) {
            return new PutMappingRequestBuilder(this, PutMappingAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<ForceMergeResponse> forceMerge(final ForceMergeRequest request) {
            return execute(ForceMergeAction.INSTANCE, request);
        }

        @Override
        public void forceMerge(final ForceMergeRequest request, final ActionListener<ForceMergeResponse> listener) {
            execute(ForceMergeAction.INSTANCE, request, listener);
        }

        @Override
        public ForceMergeRequestBuilder prepareForceMerge(String... indices) {
            return new ForceMergeRequestBuilder(this, ForceMergeAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<UpgradeResponse> upgrade(final UpgradeRequest request) {
            return execute(UpgradeAction.INSTANCE, request);
        }

        @Override
        public void upgrade(final UpgradeRequest request, final ActionListener<UpgradeResponse> listener) {
            execute(UpgradeAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<RefreshResponse> refresh(final RefreshRequest request) {
            return execute(RefreshAction.INSTANCE, request);
        }

        @Override
        public void refresh(final RefreshRequest request, final ActionListener<RefreshResponse> listener) {
            execute(RefreshAction.INSTANCE, request, listener);
        }

        @Override
        public RefreshRequestBuilder prepareRefresh(String... indices) {
            return new RefreshRequestBuilder(this, RefreshAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<IndicesStatsResponse> stats(final IndicesStatsRequest request) {
            return execute(IndicesStatsAction.INSTANCE, request);
        }

        @Override
        public void stats(final IndicesStatsRequest request, final ActionListener<IndicesStatsResponse> listener) {
            execute(IndicesStatsAction.INSTANCE, request, listener);
        }

        @Override
        public IndicesStatsRequestBuilder prepareStats(String... indices) {
            return new IndicesStatsRequestBuilder(this, IndicesStatsAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<RecoveryResponse> recoveries(final RecoveryRequest request) {
            return execute(RecoveryAction.INSTANCE, request);
        }

        @Override
        public void recoveries(final RecoveryRequest request, final ActionListener<RecoveryResponse> listener) {
            execute(RecoveryAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<AcknowledgedResponse> updateSettings(final UpdateSettingsRequest request) {
            return execute(UpdateSettingsAction.INSTANCE, request);
        }

        @Override
        public void updateSettings(final UpdateSettingsRequest request, final ActionListener<AcknowledgedResponse> listener) {
            execute(UpdateSettingsAction.INSTANCE, request, listener);
        }

        @Override
        public UpdateSettingsRequestBuilder prepareUpdateSettings(String... indices) {
            return new UpdateSettingsRequestBuilder(this, UpdateSettingsAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<AcknowledgedResponse> putTemplate(final PutIndexTemplateRequest request) {
            return execute(PutIndexTemplateAction.INSTANCE, request);
        }

        @Override
        public void putTemplate(final PutIndexTemplateRequest request, final ActionListener<AcknowledgedResponse> listener) {
            execute(PutIndexTemplateAction.INSTANCE, request, listener);
        }

        @Override
        public PutIndexTemplateRequestBuilder preparePutTemplate(String name) {
            return new PutIndexTemplateRequestBuilder(this, PutIndexTemplateAction.INSTANCE, name);
        }

        @Override
        public ActionFuture<GetIndexTemplatesResponse> getTemplates(final GetIndexTemplatesRequest request) {
            return execute(GetIndexTemplatesAction.INSTANCE, request);
        }

        @Override
        public void getTemplates(final GetIndexTemplatesRequest request, final ActionListener<GetIndexTemplatesResponse> listener) {
            execute(GetIndexTemplatesAction.INSTANCE, request, listener);
        }

        @Override
        public GetIndexTemplatesRequestBuilder prepareGetTemplates(String... names) {
            return new GetIndexTemplatesRequestBuilder(this, GetIndexTemplatesAction.INSTANCE, names);
        }

        @Override
        public void deleteTemplate(final DeleteIndexTemplateRequest request, final ActionListener<AcknowledgedResponse> listener) {
            execute(DeleteIndexTemplateAction.INSTANCE, request, listener);
        }

        @Override
        public DeleteIndexTemplateRequestBuilder prepareDeleteTemplate(String name) {
            return new DeleteIndexTemplateRequestBuilder(this, DeleteIndexTemplateAction.INSTANCE, name);
        }

        @Override
        public ActionFuture<GetSettingsResponse> getSettings(GetSettingsRequest request) {
            return execute(GetSettingsAction.INSTANCE, request);
        }

        @Override
        public void getSettings(GetSettingsRequest request, ActionListener<GetSettingsResponse> listener) {
            execute(GetSettingsAction.INSTANCE, request, listener);
        }
    }
}
