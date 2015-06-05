/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.testing;

import io.crate.action.sql.*;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.TestCluster;
import org.hamcrest.Matchers;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class SQLTransportExecutor {

    private static final ESLogger LOGGER = Loggers.getLogger(SQLTransportExecutor.class);
    private final ClientProvider clientProvider;
    private static final Long REQUEST_TIMEOUT = 5L;

    public static SQLTransportExecutor create(final TestCluster testCluster) {
        return new SQLTransportExecutor(new ClientProvider() {
            @Override
            public Client client() {
                return testCluster.client();
            }
        });
    }

    public SQLTransportExecutor(ClientProvider clientProvider) {
        this.clientProvider = clientProvider;
    }

    public SQLResponse exec(String statement) {
        return execute(statement, new Object[0]).actionGet(REQUEST_TIMEOUT, TimeUnit.SECONDS);
    }

    public SQLResponse exec(String statement, Object... params) {
        return execute(statement, params).actionGet(REQUEST_TIMEOUT, TimeUnit.SECONDS);
    }

    public SQLResponse exec(SQLRequest request) {
        return execute(request).actionGet(REQUEST_TIMEOUT, TimeUnit.SECONDS);
    }

    public SQLBulkResponse exec(String statement, Object[][] bulkArgs) {
        return execute(statement, bulkArgs).actionGet(REQUEST_TIMEOUT, TimeUnit.SECONDS);
    }

    public SQLBulkResponse exec(SQLBulkRequest request) {
        return execute(request).actionGet(REQUEST_TIMEOUT, TimeUnit.SECONDS);
    }

    public ActionFuture<SQLResponse> execute(String statement, Object[] params) {
        return clientProvider.client().execute(SQLAction.INSTANCE, new SQLRequest(statement, params));
    }

    public ActionFuture<SQLResponse> execute(SQLRequest request) {
        return clientProvider.client().execute(SQLAction.INSTANCE, request);
    }

    public ActionFuture<SQLBulkResponse> execute(String statement, Object[][] bulkArgs) {
        return clientProvider.client().execute(SQLBulkAction.INSTANCE, new SQLBulkRequest(statement, bulkArgs));
    }

    public ActionFuture<SQLBulkResponse> execute(SQLBulkRequest request) {
        return clientProvider.client().execute(SQLBulkAction.INSTANCE, request);
    }

    public ClusterHealthStatus ensureGreen() {
        return ensureState(ClusterHealthStatus.GREEN);
    }

    public ClusterHealthStatus ensureYellowOrGreen() {
        return ensureState(ClusterHealthStatus.YELLOW);
    }

    private ClusterHealthStatus ensureState(ClusterHealthStatus state) {
        ClusterHealthResponse actionGet = client().admin().cluster().health(
                Requests.clusterHealthRequest()
                        .waitForStatus(state)
                        .waitForEvents(Priority.LANGUID).waitForRelocatingShards(0)
        ).actionGet();

        if (actionGet.isTimedOut()) {
            LOGGER.info("ensure state timed out, cluster state:\n{}\n{}", client().admin().cluster().prepareState().get().getState().prettyPrint(), client().admin().cluster().preparePendingClusterTasks().get().prettyPrint());
            assertThat("timed out waiting for state", actionGet.isTimedOut(), equalTo(false));
        }
        if (state == ClusterHealthStatus.YELLOW) {
            assertThat(actionGet.getStatus(), Matchers.anyOf(equalTo(state), equalTo(ClusterHealthStatus.GREEN)));
        } else {
            assertThat(actionGet.getStatus(), equalTo(state));
        }
        return actionGet.getStatus();
    }

    public RefreshResponse refresh(String index) {
        return client().admin().indices().prepareRefresh(index).execute().actionGet();
    }

    public Client client() {
        return clientProvider.client();
    }

    public CreateIndexRequestBuilder prepareCreate(String index) {
        return client().admin().indices().prepareCreate(index);
    }

    public interface ClientProvider {
        Client client();
    }
}
