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

package org.elasticsearch.cluster;

import static io.crate.testing.SQLTransportExecutor.REQUEST_TIMEOUT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.test.TestCluster;
import org.junit.Test;

import org.elasticsearch.test.IntegTestCase;

public class ClusterHealthIT extends IntegTestCase {

    @Test
    public void testSimpleLocalHealth() {
        execute("create table test (id int) with (number_of_replicas = 0)");
        ensureGreen(); // master should think it's green now.

        for (final String node : internalCluster().getNodeNames()) {
            // a very high time out, which should never fire due to the local flag
            logger.info("--> getting cluster health on [{}]", node);
            final ClusterHealthResponse health = FutureUtils.get(client(node).admin().cluster().health(
                new ClusterHealthRequest()
                    .local(true)
                    .waitForEvents(Priority.LANGUID)
                    .timeout("30s")
                ), REQUEST_TIMEOUT);
            logger.info("--> got cluster health on [{}]", node);
            assertFalse("timed out on " + node, health.isTimedOut());
            assertThat("health status on " + node, health.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
    }

    @Test
    public void testHealth() {
        logger.info("--> running cluster health on an index that does not exists");
        ClusterHealthResponse healthResponse = FutureUtils.get(client().admin().cluster().health(
            new ClusterHealthRequest("test1")
                .waitForYellowStatus()
                .timeout("1s")
            ));
        assertThat(healthResponse.isTimedOut(), equalTo(true));
        assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.RED));
        assertThat(healthResponse.getIndices().isEmpty(), equalTo(true));

        logger.info("--> running cluster wide health");
        healthResponse = FutureUtils.get(client().admin().cluster().health(
            new ClusterHealthRequest()
                .waitForGreenStatus()
                .timeout("10s")
            ));
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(healthResponse.getIndices().isEmpty(), equalTo(true));

        logger.info("--> Creating index test1 with zero replicas");
        createIndex("test1");

        logger.info("--> running cluster health on an index that does exists");
        healthResponse = FutureUtils.get(client().admin().cluster().health(
            new ClusterHealthRequest("test1")
                .waitForGreenStatus()
                .timeout("10s")
            ));
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(healthResponse.getIndices().get("test1").getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> running cluster health on an index that does exists and an index that doesn't exists");
        healthResponse = FutureUtils.get(client().admin().cluster().health(
            new ClusterHealthRequest("test1", "test2")
                .waitForYellowStatus()
                .timeout("1s")
            ));
        assertThat(healthResponse.isTimedOut(), equalTo(true));
        assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.RED));
        assertThat(healthResponse.getIndices().get("test1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(healthResponse.getIndices().size(), equalTo(1));
    }

    @Test
    public void testHealthWithClosedIndices() throws Exception {
        var table_1 = getFqn("t1");
        execute("create table t1 (id int) with (number_of_replicas = 0)");

        var table_2 = getFqn("t2");
        execute("create table t2 (id int) with (number_of_replicas = 0)");
        waitNoPendingTasksOnAll();
        execute("alter table t2 close");

        var table_3 = getFqn("t3");
        execute("create table t3 (id int) with (number_of_replicas = 20)");
        waitNoPendingTasksOnAll();
        execute("alter table t3 close");

        {
            ClusterHealthResponse response = FutureUtils.get(client().admin().cluster().health(
                new ClusterHealthRequest()
                    .waitForNoRelocatingShards(true)
                    .waitForNoInitializingShards(true)
                    .waitForYellowStatus()
                ), REQUEST_TIMEOUT);
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(3));
            assertThat(response.getIndices().get(table_1).getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get(table_2).getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get(table_3).getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        }
        {
            ClusterHealthResponse response = FutureUtils.get(
                client().admin().cluster().health(new ClusterHealthRequest(table_1)),
                REQUEST_TIMEOUT
            );
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get(table_1).getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
        {
            ClusterHealthResponse response = FutureUtils.get(
                client().admin().cluster().health(new ClusterHealthRequest(table_2)),
                REQUEST_TIMEOUT
            );
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get(table_2).getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
        {
            ClusterHealthResponse response = FutureUtils.get(
                client().admin().cluster().health(new ClusterHealthRequest(table_3)),
                REQUEST_TIMEOUT
            );
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get(table_3).getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        }

        execute("alter table t3 set (number_of_replicas = ?)", new Object[] { numberOfReplicas() });

        {
            ClusterHealthResponse response = FutureUtils.get(
                client().admin().cluster().health(new ClusterHealthRequest().waitForGreenStatus()),
                REQUEST_TIMEOUT
            );
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(3));
            assertThat(response.getIndices().get(table_1).getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get(table_2).getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get(table_3).getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
    }

    @Test
    public void testHealthOnIndexCreation() throws Exception {
        final AtomicBoolean finished = new AtomicBoolean(false);
        Thread clusterHealthThread = new Thread() {
            @Override
            public void run() {
                while (finished.get() == false) {
                    ClusterHealthResponse health = FutureUtils.get(
                        client().admin().cluster().health(new ClusterHealthRequest()));
                    assertThat(health.getStatus(), not(equalTo(ClusterHealthStatus.RED)));
                }
            }
        };
        clusterHealthThread.start();
        for (int i = 0; i < 10; i++) {
            createIndex("test" + i);
        }
        finished.set(true);
        clusterHealthThread.join();
    }

    @Test
    public void testWaitForEventsRetriesIfOtherConditionsNotMet() throws Exception {
        final CompletableFuture<ClusterHealthResponse> healthResponseFuture = client().admin().cluster().health(
            new ClusterHealthRequest("index")
                .waitForEvents(Priority.LANGUID)
                .waitForGreenStatus()
            );

        final AtomicBoolean keepSubmittingTasks = new AtomicBoolean(true);
        final ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        clusterService.submitStateUpdateTask("looping task", new ClusterStateUpdateTask(Priority.LOW) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return currentState;
                }

                @Override
                public void onFailure(String source, Exception e) {
                    throw new AssertionError(source, e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    if (keepSubmittingTasks.get()) {
                        clusterService.submitStateUpdateTask("looping task", this);
                    }
                }
            });

        createIndex("index");
        var clusterHealthResponse = FutureUtils.get(client().admin().cluster().health(new ClusterHealthRequest("index").waitForGreenStatus()));
        assertFalse(clusterHealthResponse.isTimedOut());

        // at this point the original health response should not have returned: there was never a point where the index was green AND
        // the master had processed all pending tasks above LANGUID priority.
        assertFalse(healthResponseFuture.isDone());

        keepSubmittingTasks.set(false);
        assertFalse(healthResponseFuture.get().isTimedOut());
    }

    @Test
    public void testHealthOnMasterFailover() throws Exception {
        final String node = internalCluster().startDataOnlyNode();
        final List<CompletableFuture<ClusterHealthResponse>> responseFutures = new ArrayList<>();
        // Run a few health requests concurrent to master fail-overs against a data-node to make sure master failover is handled
        // without exceptions
        for (int i = 0; i < 20; ++i) {
            responseFutures.add(client(node).admin().cluster().health(new ClusterHealthRequest().waitForEvents(Priority.LANGUID)));
            internalCluster().restartNode(internalCluster().getMasterName(), TestCluster.EMPTY_CALLBACK);
        }
        for (var responseFuture : responseFutures) {
            assertSame(responseFuture.get().getStatus(), ClusterHealthStatus.GREEN);
        }
    }
}
