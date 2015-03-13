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

package io.crate.action.sql.query;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.analyze.WhereClause;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.ReferenceInfo;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateIntegrationTest;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.query.QuerySearchResult;
import org.hamcrest.Matchers;
import org.junit.Test;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class TransportQueryShardActionTest extends SQLTransportIntegrationTest {


    @Test
    public void testQueryShardRequestHandling() throws Exception {
        // this test just verifies that the request handler is hooked up correctly.
        // once the queryShardAction is used somewhere this test can be deleted.

        TransportQueryShardAction queryShardAction = cluster().getInstance(TransportQueryShardAction.class);
        ClusterService clusterService = cluster().getInstance(ClusterService.class);

        final SettableFuture<Boolean> response = SettableFuture.create();

        DiscoveryNode[] discoveryNodes = clusterService.state().nodes().nodes().values().toArray(DiscoveryNode.class);
        queryShardAction.executeQuery(
                discoveryNodes[1].id(),
                new QueryShardRequest("foo",
                        1,
                        ImmutableList.<Reference>of(),
                        ImmutableList.<Symbol>of(),
                        new boolean[0],
                        new Boolean[0],
                        10,
                        0,
                        WhereClause.MATCH_ALL,
                        ImmutableList.<ReferenceInfo>of(),
                        Optional.<TimeValue>absent()
                ),
                new ActionListener<QuerySearchResult>() {
                    @Override
                    public void onResponse(QuerySearchResult queryShardResponse) {
                        response.set(false);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        response.set(true);
                    }
                });

        assertThat(response.get(), Matchers.is(true));
    }
}