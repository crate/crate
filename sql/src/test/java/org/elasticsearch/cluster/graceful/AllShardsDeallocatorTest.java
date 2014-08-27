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

package org.elasticsearch.cluster.graceful;

import com.google.common.util.concurrent.ListenableFuture;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.test.integration.CrateIntegrationTest;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.Loggers;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.TEST, numNodes=3)
public class AllShardsDeallocatorTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
        Loggers.getLogger(AllShardsDeallocator.class).setLevel("TRACE");
        Loggers.getLogger(TransportClusterUpdateSettingsAction.class).setLevel("TRACE");
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private DiscoveryNode takeDownNode;

    private Random random = new Random(System.currentTimeMillis());


    @Before
    public void prepare() {
        RoutingNode[] nodes = clusterService().state().routingNodes().toArray();
        takeDownNode = nodes[random.nextInt(nodes.length)].node();
    }

    @Test
    public void testDeallocate() throws Exception {
        execute("create table t0 (id int primary key, name string) clustered into 2 shards with (number_of_replicas=0)");
        execute("create table t1 (id int primary key, name string) clustered into 2 shards with (number_of_replicas=1)");
        ensureGreen();

        String tmpl = "insert into %s (id, name) values (%d, '%s')";
        for (String table : Arrays.asList("t0", "t1")) {
            execute(String.format(Locale.ENGLISH, tmpl, table, random.nextInt(), Strings.randomBase64UUID(random)));
            execute(String.format(Locale.ENGLISH, tmpl, table, random.nextInt(), Strings.randomBase64UUID(random)));
        }
        refresh();

        AllShardsDeallocator allShardsDeallocator = cluster().getInstance(AllShardsDeallocator.class, takeDownNode.name());
        ListenableFuture<Deallocator.DeallocationResult> future = allShardsDeallocator.deallocate();
        Deallocator.DeallocationResult result = future.get(1, TimeUnit.MINUTES);
        assertThat(result.success(), is(true));
        assertThat(result.didDeallocate(), is(true));


        assertThat(
                cluster().getInstance(ClusterService.class, takeDownNode.name()).state().routingNodes().node(takeDownNode.id()).size(),
                is(0));
    }

    @Test
    public void testDeallocateFailCannotMoveShards() throws Exception {
        execute("create table t2 (id int primary key, name string) clustered into 2 shards with (number_of_replicas=2)");
        ensureGreen();

        execute(String.format(Locale.ENGLISH, "insert into t2 (id, name) values (%d, '%s')", random.nextInt(), Strings.randomBase64UUID(random)));
        execute(String.format(Locale.ENGLISH, "insert into t2 (id, name) values (%d, '%s')", random.nextInt(), Strings.randomBase64UUID(random)));

        refresh();

        AllShardsDeallocator allShardsDeallocator = cluster().getInstance(AllShardsDeallocator.class, takeDownNode.name());
        ListenableFuture<Deallocator.DeallocationResult> future = allShardsDeallocator.deallocate();
        try {
            future.get(2, TimeUnit.SECONDS);
            fail("no TimeoutException occurred");
        } catch (TimeoutException e) {
            assertThat(clusterService().state().routingNodes().node(takeDownNode.id()).size(), is(2));
        }
    }

    @Test
    public void testCancel() throws Exception {
        execute("create table t0 (id int primary key, name string) clustered into 2 shards with (number_of_replicas=0)");
        execute("create table t1 (id int primary key, name string) clustered into 2 shards with (number_of_replicas=1)");
        ensureGreen();

        String tmpl = "insert into %s (id, name) values (%d, '%s')";
        for (String table : Arrays.asList("t0", "t1")) {
            for (int i = 0; i<10; i++) {
                execute(String.format(Locale.ENGLISH, tmpl, table, random.nextInt(), Strings.randomBase64UUID(random)));
            }
        }
        refresh();

        AllShardsDeallocator allShardsDeallocator = cluster().getInstance(AllShardsDeallocator.class, takeDownNode.name());
        assertThat(allShardsDeallocator.cancel(), is(false));
        ListenableFuture<Deallocator.DeallocationResult> future = allShardsDeallocator.deallocate();
        assertThat(allShardsDeallocator.isDeallocating(), is(true));
        assertThat(allShardsDeallocator.cancel(), is(true));
        assertThat(allShardsDeallocator.isDeallocating(), is(false));

        expectedException.expect(ExecutionException.class);
        expectedException.expectCause(Matchers.<Throwable>instanceOf(DeallocationCancelledException.class));

        future.get(1, TimeUnit.SECONDS);
    }

    @Test
    public void testDeallocationNoOps() throws Exception {
        AllShardsDeallocator allShardsDeallocator = cluster().getInstance(AllShardsDeallocator.class, takeDownNode.name());

        // NOOP
        Deallocator.DeallocationResult result = allShardsDeallocator.deallocate().get(1, TimeUnit.SECONDS);
        assertThat(result.success(), is(true));
        assertThat(result.didDeallocate(), is(false));

        execute("create table t0 (id int primary key, name string) clustered into 2 shards with (number_of_replicas=0)");
        execute("create table t1 (id int primary key, name string) clustered into 2 shards with (number_of_replicas=1)");
        ensureGreen();

        String tmpl = "insert into %s (id, name) values (%d, '%s')";
        for (String table : Arrays.asList("t0", "t1")) {
            for (int i = 0; i<10; i++) {
                execute(String.format(Locale.ENGLISH, tmpl, table, random.nextInt(), Strings.randomBase64UUID(random)));
            }
        }
        refresh();

        // DOUBLE deallocation
        allShardsDeallocator.deallocate();

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("node already waiting for complete deallocation");
        allShardsDeallocator.deallocate();
    }
}
