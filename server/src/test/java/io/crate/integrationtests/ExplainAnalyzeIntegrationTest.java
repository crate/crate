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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.testing.T3;
import io.crate.testing.UseRandomizedOptimizerRules;

@IntegTestCase.ClusterScope(numDataNodes = 2)
public class ExplainAnalyzeIntegrationTest extends IntegTestCase {

    @Before
    public void initTestData() throws Exception {
        Setup setup = new Setup(sqlExecutor);
        setup.setUpLocations();
        execute("refresh table locations");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testExplainAnalyzeReportsExecutionTimesOnBothNodesInclQueryBreakdown() {
        execute("explain analyze select * from locations where name like 'a%' or name = 'foo' order by date desc");

        Map<String, Object> analysis = (Map<String, Object>) response.rows()[0][0];
        Map<String, Object> executeAnalysis = (Map<String, Object>) analysis.get("Execute");

        assertThat(executeAnalysis).containsKeys("Phases", "Total");

        Map<String, Map<String, Object>> phasesAnalysis = (Map<String, Map<String, Object>>) executeAnalysis.get("Phases");
        assertThat(phasesAnalysis).isNotNull();
        assertThat(phasesAnalysis.keySet()).containsExactly("0-collect", "1-mergeOnHandler", "2-fetchPhase");

        DiscoveryNodes nodes = clusterService().state().nodes();
        for (DiscoveryNode discoveryNode : nodes) {
            if (discoveryNode.isDataNode()) {
                Object actual = executeAnalysis.get(discoveryNode.getId());
                assertThat(actual).isInstanceOf(Map.class);

                Map<String, Object> timings = (Map<String, Object>) actual;
                assertThat(timings).containsOnlyKeys("QueryBreakdown");

                Map<String, String> queryBreakdown = ((List<Map<String, String>>) timings.get("QueryBreakdown")).getFirst();
                assertThat(queryBreakdown).containsEntry("QueryName", "ConstantScoreQuery");
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testExplainSelectWithoutJobExecutionContexts() {
        execute("explain analyze select 1");
        Map<String, Object> analysis = (Map<String, Object>) response.rows()[0][0];
        Map<String, Object> executeAnalysis = (Map<String, Object>) analysis.get("Execute");
        assertThat(executeAnalysis).containsKeys("Phases", "Total");
        DiscoveryNodes nodes = clusterService().state().nodes();
        List<String> nodeIds = new ArrayList<>(nodes.getSize());
        for (DiscoveryNode discoveryNode : nodes) {
            nodeIds.add(discoveryNode.getId());
        }
        assertThat(executeAnalysis)
            .containsKeys("Total")
            .satisfies(m -> assertThat(m.keySet()).containsAnyOf(nodeIds.toArray(new String[] {})));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test_explain_analyze_on_statement_with_subquery() {
        execute("explain analyze select * from locations where id = (select id from locations where name = 'foo')");
        Map<String, Object> analysis = (Map<String, Object>) response.rows()[0][0];
        Map<String, Object> executeAnalysis = (Map<String, Object>) analysis.get("Execute");
        List<Object> subPlans = (List<Object>) executeAnalysis.get("Sub-Queries");
        assertThat(subPlans).hasSize(1);
        Map<String, Object> subPlanAnalysis = (Map<String, Object>) subPlans.getFirst();
        assertThat(subPlanAnalysis).containsKeys("Phases", "Total");
        Map<String, Map<String, Object>> phasesAnalysis = (Map<String, Map<String, Object>>) subPlanAnalysis.get("Phases");
        assertThat(phasesAnalysis).isNotNull();
        assertThat(phasesAnalysis.keySet()).containsExactly("0-collect", "1-mergeOnHandler");

        DiscoveryNodes nodes = clusterService().state().nodes();
        for (DiscoveryNode discoveryNode : nodes) {
            if (discoveryNode.isDataNode()) {
                Object actual = subPlanAnalysis.get(discoveryNode.getId());
                assertThat(actual).isInstanceOf(Map.class);

                Map<String, Object> timings = (Map<String, Object>) actual;
                assertThat(timings).containsOnlyKeys("QueryBreakdown");

                Map<String, String> queryBreakdown = ((List<Map<String, String>>) timings.get("QueryBreakdown")).getFirst();
                assertThat(queryBreakdown).containsEntry("QueryName", "TermQuery");
            }
        }
    }

    @UseRandomizedOptimizerRules(0)
    @SuppressWarnings("unchecked")
    @Test
    public void test_explain_analyze_with_nested_subquery() {
        execute(T3.T1_DEFINITION);
        execute(T3.T2_DEFINITION);
        execute("EXPLAIN ANALYZE " +
            "SELECT * FROM doc.t1 WHERE x > (SELECT 1 FROM doc.t1 WHERE x > (SELECT count(*) FROM doc.t2 LIMIT 1)::integer)");
        Map<String, Object> analysis = (Map<String, Object>) response.rows()[0][0];
        Map<String, Object> executeAnalysis = (Map<String, Object>) analysis.get("Execute");

        List<Object> subPlans = (List<Object>) executeAnalysis.get("Sub-Queries");
        assertThat(subPlans).hasSize(1);
        Map<String, Object> subPlanAnalysis = (Map<String, Object>) subPlans.getFirst();
        assertThat(subPlanAnalysis).containsKeys("Phases", "Total");
        Map<String, Map<String, Object>> phasesAnalysis = (Map<String, Map<String, Object>>) subPlanAnalysis.get("Phases");
        assertThat(phasesAnalysis).isNotNull();
        assertThat(phasesAnalysis.keySet()).contains("0-collect");

        List<Object> subSubPlans = (List<Object>) subPlanAnalysis.get("Sub-Queries");
        assertThat(subSubPlans).hasSize(1);
        Map<String, Object> subSubPlanAnalysis = (Map<String, Object>) subSubPlans.getFirst();
        assertThat(subSubPlanAnalysis).containsKeys("Phases", "Total");
        Map<String, Map<String, Object>> subPhasesAnalysis = (Map<String, Map<String, Object>>) subSubPlanAnalysis.get("Phases");
        assertThat(subPhasesAnalysis).isNotNull();
        assertThat(subPhasesAnalysis.keySet()).contains("0-count");
    }

    @Test
    public void test_explain_analyze_query_execution_contains_shard_information() {
        execute("EXPLAIN ANALYZE SELECT * FROM locations");
        Map<String, Object> analysis = (Map<String, Object>) response.rows()[0][0];
        Map<String, Object> executeAnalysis = (Map<String, Object>) analysis.get("Execute");

        DiscoveryNodes nodes = clusterService().state().nodes();
        for (DiscoveryNode discoveryNode : nodes) {
            if (discoveryNode.isDataNode()) {
                Object actual = executeAnalysis.get(discoveryNode.getId());
                assertThat(actual).isInstanceOf(Map.class);

                Map<String, Object> timings = (Map<String, Object>) actual;
                Map<String, Object> queryBreakdown = ((List<Map<String, Object>>) timings.get("QueryBreakdown")).getFirst();
                assertThat(queryBreakdown.get("SchemaName")).isNotNull();
                assertThat(queryBreakdown.get("TableName")).isEqualTo("locations");
                assertThat((int) queryBreakdown.get("ShardId")).isGreaterThanOrEqualTo(0);
                assertThat(queryBreakdown).doesNotContainKey("PartitionIdent");
            }
        }
    }

    @Test
    public void test_explain_analyze_query_execution_contains_shard_and_partition_information() {
        execute("CREATE TABLE my_schema.parted (id int, p int) PARTITIONED BY (p)");
        execute("INSERT INTO my_schema.parted (id, p) VALUES (1, 0)");
        execute("EXPLAIN ANALYZE SELECT * FROM my_schema.parted");
        Map<String, Object> analysis = (Map<String, Object>) response.rows()[0][0];
        Map<String, Object> executeAnalysis = (Map<String, Object>) analysis.get("Execute");

        DiscoveryNodes nodes = clusterService().state().nodes();
        for (DiscoveryNode discoveryNode : nodes) {
            if (discoveryNode.isDataNode()) {
                Object actual = executeAnalysis.get(discoveryNode.getId());
                assertThat(actual).isInstanceOf(Map.class);

                Map<String, Object> timings = (Map<String, Object>) actual;
                Map<String, Object> queryBreakdown = ((List<Map<String, Object>>) timings.get("QueryBreakdown")).getFirst();
                assertThat(queryBreakdown.get("SchemaName")).isEqualTo("my_schema");
                assertThat(queryBreakdown.get("TableName")).isEqualTo("parted");
                assertThat((int) queryBreakdown.get("ShardId")).isGreaterThanOrEqualTo(0);
                assertThat(queryBreakdown.get("PartitionIdent")).isEqualTo("04130");
            }
        }
    }
}
