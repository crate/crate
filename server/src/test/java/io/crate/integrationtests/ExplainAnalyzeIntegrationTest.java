/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.integrationtests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.IntegTestCase;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

@IntegTestCase.ClusterScope(numDataNodes = 2)
public class ExplainAnalyzeIntegrationTest extends IntegTestCase {

    @Before
    public void initTestData() throws Exception {
        Setup setup = new Setup(sqlExecutor);
        setup.setUpLocations();
        execute("refresh table locations");
    }

    @Test
    public void testExplainAnalyzeReportsExecutionTimesOnBothNodesInclQueryBreakdown() {
        execute("explain analyze select * from locations where name like 'a%' or name = 'foo' order by date desc");

        Map<String, Object> analysis = (Map<String, Object>) response.rows()[0][0];
        Map<String, Object> executeAnalysis = (Map<String, Object>) analysis.get("Execute");

        assertThat(executeAnalysis).isNotNull();
        assertTrue(executeAnalysis.keySet().contains("Total"));

        Map<String, Map<String, Object>> phasesAnalysis = (Map<String, Map<String, Object>>) executeAnalysis.get("Phases");
        assertThat(phasesAnalysis).isNotNull();
        assertThat(phasesAnalysis.keySet()).containsExactly("0-collect", "1-mergeOnHandler", "2-fetchPhase");

        DiscoveryNodes nodes = clusterService().state().nodes();
        for (DiscoveryNode discoveryNode : nodes) {
            if (discoveryNode.isDataNode()) {
                Object actual = executeAnalysis.get(discoveryNode.getId());
                assertThat(actual).isInstanceOf(Map.class);

                Map<String, Object> timings = (Map) actual;
                assertThat(timings, Matchers.hasKey("QueryBreakdown"));

                Map<String, String> queryBreakdown = ((Map) ((List) timings.get("QueryBreakdown")).get(0));
                assertThat(queryBreakdown, Matchers.hasEntry(is("QueryName"), is("ConstantScoreQuery")));
            }
        }
    }

    @Test
    public void testExplainSelectWithoutJobExecutionContexts() {
        execute("explain analyze select 1");
        Map<String, Object> analysis = (Map<String, Object>) response.rows()[0][0];
        Map<String, Object> executeAnalysis = (Map<String, Object>) analysis.get("Execute");
        assertTrue(executeAnalysis.keySet().contains("Total"));
        DiscoveryNodes nodes = clusterService().state().nodes();
        List<Matcher<String>> nodeIds = new ArrayList<>(nodes.getSize());
        for (DiscoveryNode discoveryNode : nodes) {
            nodeIds.add(is(discoveryNode.getId()));
        }
        assertThat(executeAnalysis.keySet(), hasItems(is("Total"), anyOf(nodeIds.toArray(new Matcher[]{}))));
    }
}
