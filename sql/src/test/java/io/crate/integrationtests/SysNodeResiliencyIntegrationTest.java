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

package io.crate.integrationtests;

import com.google.common.collect.Sets;
import io.crate.action.sql.SQLOperations;
import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;
import io.crate.testing.SQLTransportExecutor;
import org.elasticsearch.common.Strings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.NetworkPartition;
import org.elasticsearch.test.disruption.NetworkUnresponsivePartition;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.Matchers.*;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2, transportClientRatio = 0)
public class SysNodeResiliencyIntegrationTest extends SQLTransportIntegrationTest {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        ArrayList<Class<? extends Plugin>> nodePlugins = new ArrayList<>(super.nodePlugins());
        nodePlugins.add(MockTransportService.TestPlugin.class);
        return nodePlugins;
    }


    /**
     * Test that basic information from cluster state is used if a sys node
     * request is timing out
     */
    @Test
    public void testTimingOutNode() throws Exception {
        final List<String> nodes = Arrays.asList(internalCluster().getNodeNames());
        final String unluckyNode = randomFrom(nodes);
        final Set<String> luckyNodes = new HashSet<>(nodes);
        luckyNodes.remove(unluckyNode);

        NetworkPartition partition
            = new NetworkUnresponsivePartition(luckyNodes, Sets.newHashSet(unluckyNode), getRandom());
        setDisruptionScheme(partition);
        partition.startDisrupting();

        SQLRequest request = new SQLRequest("select version, hostname, id, name from sys.nodes where name = ?", new Object[]{unluckyNode});

        SQLOperations sqlOperations = internalCluster().getInstance(SQLOperations.class,
            randomFrom(luckyNodes.toArray(Strings.EMPTY_ARRAY)));
        assert sqlOperations != null;
        SQLResponse response = SQLTransportExecutor.execute(request, sqlOperations).actionGet(SQLTransportExecutor.REQUEST_TIMEOUT);

        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is(nullValue()));
        assertThat(response.rows()[0][1], is(nullValue()));
        assertThat(response.rows()[0][2], is(notNullValue()));
        assertThat((String) response.rows()[0][3], is(unluckyNode));
    }

    @Test
    public void testNoMatchingNode() throws Exception {
        execute("select id, name, hostname from sys.nodes where id = 'does-not-exist'");
        assertThat(response.rowCount(), is(0L));
    }
}
