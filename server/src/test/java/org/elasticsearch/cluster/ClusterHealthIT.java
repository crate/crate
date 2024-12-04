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
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.TestCluster;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
import io.crate.metadata.RelationName;
import io.crate.testing.UseRandomizedSchema;

public class ClusterHealthIT extends IntegTestCase {

    @Test
    public void testSimpleLocalHealth() {
        execute("create table test (id int) with (number_of_replicas = 0)");
        ensureGreen(); // master should think it's green now.

        for (final String node : cluster().getNodeNames()) {
            // a very high time out, which should never fire due to the local flag
            logger.info("--> getting cluster health on [{}]", node);
            final ClusterHealthResponse health = FutureUtils.get(client(node).admin().cluster().health(
                new ClusterHealthRequest()
                    .local(true)
                    .waitForEvents(Priority.LANGUID)
                    .timeout("30s")
                ), REQUEST_TIMEOUT);
            logger.info("--> got cluster health on [{}]", node);
            assertThat(health.isTimedOut()).as("timed out on " + node).isFalse();
            assertThat(health.getStatus()).as("health status on " + node).isEqualTo(ClusterHealthStatus.GREEN);
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
        assertThat(healthResponse.isTimedOut()).isTrue();
        assertThat(healthResponse.getStatus()).isEqualTo(ClusterHealthStatus.RED);
        assertThat(healthResponse.getIndices().isEmpty()).isTrue();

        logger.info("--> running cluster wide health");
        healthResponse = FutureUtils.get(client().admin().cluster().health(
            new ClusterHealthRequest()
                .waitForGreenStatus()
                .timeout("10s")
            ));
        assertThat(healthResponse.isTimedOut()).isFalse();
        assertThat(healthResponse.getStatus()).isEqualTo(ClusterHealthStatus.GREEN);
        assertThat(healthResponse.getIndices().isEmpty()).isTrue();

        logger.info("--> Creating index test1 with zero replicas");
        execute("create table doc.test1 (x int) with (number_of_replicas = 0)");

        logger.info("--> running cluster health on an index that does exists");
        healthResponse = FutureUtils.get(client().admin().cluster().health(
            new ClusterHealthRequest("test1")
                .waitForGreenStatus()
                .timeout("10s")
            ));
        assertThat(healthResponse.isTimedOut()).isFalse();
        assertThat(healthResponse.getStatus()).isEqualTo(ClusterHealthStatus.GREEN);
        assertThat(healthResponse.getIndices().get("test1").getStatus()).isEqualTo(ClusterHealthStatus.GREEN);

        logger.info("--> running cluster health on an index that does exists and an index that doesn't exists");
        healthResponse = FutureUtils.get(client().admin().cluster().health(
            new ClusterHealthRequest("test1", "test2")
                .waitForYellowStatus()
                .timeout("1s")
            ));
        assertThat(healthResponse.isTimedOut()).isTrue();
        assertThat(healthResponse.getStatus()).isEqualTo(ClusterHealthStatus.RED);
        assertThat(healthResponse.getIndices().get("test1").getStatus()).isEqualTo(ClusterHealthStatus.GREEN);
        assertThat(healthResponse.getIndices()).hasSize(1);
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
        execute("create table t3 (id int) clustered into 1 shards with (number_of_replicas = 20)");
        waitNoPendingTasksOnAll();
        execute("alter table t3 close");

        {
            ClusterHealthResponse response = FutureUtils.get(client().admin().cluster().health(
                new ClusterHealthRequest()
                    .waitForNoRelocatingShards(true)
                    .waitForNoInitializingShards(true)
                    .waitForYellowStatus()
                ), REQUEST_TIMEOUT);
            assertThat(response.getStatus()).isEqualTo(ClusterHealthStatus.YELLOW);
            assertThat(response.isTimedOut()).isFalse();
            assertThat(response.getIndices()).hasSize(3);
            assertThat(response.getIndices().get(table_1).getStatus()).isEqualTo(ClusterHealthStatus.GREEN);
            assertThat(response.getIndices().get(table_2).getStatus()).isEqualTo(ClusterHealthStatus.GREEN);
            assertThat(response.getIndices().get(table_3).getStatus()).isEqualTo(ClusterHealthStatus.YELLOW);
        }
        {
            ClusterHealthResponse response = FutureUtils.get(
                client().admin().cluster().health(new ClusterHealthRequest(table_1)),
                REQUEST_TIMEOUT
            );
            assertThat(response.getStatus()).isEqualTo(ClusterHealthStatus.GREEN);
            assertThat(response.isTimedOut()).isFalse();
            assertThat(response.getIndices()).hasSize(1);
            assertThat(response.getIndices().get(table_1).getStatus()).isEqualTo(ClusterHealthStatus.GREEN);
        }
        {
            ClusterHealthResponse response = FutureUtils.get(
                client().admin().cluster().health(new ClusterHealthRequest(table_2)),
                REQUEST_TIMEOUT
            );
            assertThat(response.getStatus()).isEqualTo(ClusterHealthStatus.GREEN);
            assertThat(response.isTimedOut()).isFalse();
            assertThat(response.getIndices()).hasSize(1);
            assertThat(response.getIndices().get(table_2).getStatus()).isEqualTo(ClusterHealthStatus.GREEN);
        }
        {
            ClusterHealthResponse response = FutureUtils.get(
                client().admin().cluster().health(new ClusterHealthRequest(table_3)),
                REQUEST_TIMEOUT
            );
            assertThat(response.getStatus()).isEqualTo(ClusterHealthStatus.YELLOW);
            assertThat(response.isTimedOut()).isFalse();
            assertThat(response.getIndices()).hasSize(1);
            assertThat(response.getIndices().get(table_3).getStatus()).isEqualTo(ClusterHealthStatus.YELLOW);
        }

        execute("alter table t3 set (number_of_replicas = ?)", new Object[] { numberOfReplicas() });

        {
            ClusterHealthResponse response = FutureUtils.get(
                client().admin().cluster().health(new ClusterHealthRequest().waitForGreenStatus()),
                REQUEST_TIMEOUT
            );
            assertThat(response.getStatus()).isEqualTo(ClusterHealthStatus.GREEN);
            assertThat(response.isTimedOut()).isFalse();
            assertThat(response.getIndices()).hasSize(3);
            assertThat(response.getIndices().get(table_1).getStatus()).isEqualTo(ClusterHealthStatus.GREEN);
            assertThat(response.getIndices().get(table_2).getStatus()).isEqualTo(ClusterHealthStatus.GREEN);
            assertThat(response.getIndices().get(table_3).getStatus()).isEqualTo(ClusterHealthStatus.GREEN);
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
                    assertThat(health.getStatus()).isNotEqualTo(ClusterHealthStatus.RED);
                }
            }
        };
        clusterHealthThread.start();
        for (int i = 0; i < 10; i++) {
            execute("create table doc.test" + i + " (x int) with (number_of_replicas = 0)");
        }
        finished.set(true);
        clusterHealthThread.join();
    }

    @Test
    public void testWaitForEventsRetriesIfOtherConditionsNotMet() throws Exception {
        final CompletableFuture<ClusterHealthResponse> healthResponseFuture = client().admin().cluster().health(
            new ClusterHealthRequest("tbl")
                .waitForEvents(Priority.LANGUID)
                .waitForGreenStatus()
            );

        final AtomicBoolean keepSubmittingTasks = new AtomicBoolean(true);
        final ClusterService clusterService = cluster().getInstance(ClusterService.class, cluster().getMasterName());
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

        execute("create table doc.tbl (x int) with (number_of_replicas = 0)");
        var clusterHealthResponse = FutureUtils.get(client().admin().cluster().health(new ClusterHealthRequest("tbl").waitForGreenStatus()));
        assertThat(clusterHealthResponse.isTimedOut()).isFalse();

        // at this point the original health response should not have returned: there was never a point where the index was green AND
        // the master had processed all pending tasks above LANGUID priority.
        assertThat(healthResponseFuture.isDone()).isFalse();

        keepSubmittingTasks.set(false);
        assertThat(healthResponseFuture.get().isTimedOut()).isFalse();
    }

    @Test
    @UseRandomizedSchema(random = false)
    public void testHealthOnMasterFailover() throws Exception {
        final String node = cluster().startDataOnlyNode();
        boolean withIndex = randomBoolean();
        String indexName = null;
        if (withIndex) {
            // Create index with many shards to provoke the health request to wait (for green) while master is being shut down.
            // Notice that this is set to 0 after the test completed starting a number of health requests and master restarts.
            // This ensures that the cluster is yellow when the health request is made, making the health request wait on the observer,
            // triggering a call to observer.onClusterServiceClose when master is shutdown.
            indexName = "test";
            int replicas = randomIntBetween(0, 10);
            execute(String.format(Locale.ENGLISH,
                "create table %s(x text) with (number_of_replicas = %d, \"unassigned.node_left.delayed_timeout\" = '5m')", indexName, replicas)
            );
        }
        final List<CompletableFuture<ClusterHealthResponse>> responseFutures = new ArrayList<>();
        // Run a few health requests concurrent to master fail-overs against a data-node to make sure master failover is handled
        // without exceptions
        final int iterations = withIndex ? 10 : 20;
        for (int i = 0; i < iterations; ++i) {
            responseFutures.add(client(node).admin().cluster().health(new ClusterHealthRequest().waitForEvents(Priority.LANGUID)
                .waitForGreenStatus().masterNodeTimeout(TimeValue.timeValueMinutes(1))));
            cluster().restartNode(cluster().getMasterName(), TestCluster.EMPTY_CALLBACK);
        }
        if (withIndex) {
            RelationName relationName = RelationName.fromIndexName(indexName);
            String stmt = String.format(
                Locale.ENGLISH,
                "alter table \"%s\".\"%s\" set (number_of_replicas = 0)",
                relationName.schema(),
                relationName.name()
            );
            execute(stmt);
        }
        for (var responseFuture : responseFutures) {
            assertThat(responseFuture.get().getStatus()).isSameAs(ClusterHealthStatus.GREEN);
        }
    }
}
