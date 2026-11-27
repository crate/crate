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
    @UseRandomizedOptimizerRules(0)
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
        assertThat(phasesAnalysis.keySet()).containsExactly("0-collect", "1-mergeOnHandler", "2-fetchPhase");

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
        execute("CREATE TABLE my_schema.parted (id int, p int) PARTITIONED BY (p) with (number_of_replicas = 0)");
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

    @Test
    public void test_explain_analyze_with_all_values_sub_query_does_not_leak_memory() throws Exception {
        // Used to fail with something like:
        //
        //      [Query breaker not reset to 0 on node: node_s0]
        //      expected: 0L
        //      but was: 2097152L
        //
        // Because RamAccounting instance wasn't closed
        execute("explain analyze select * from sys.summits where height in (select x from unnest([1, 2, 3, 4]) t (x))");
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> collectAllQueryBreakdowns(Map<String, Object> executeAnalysis) {
        List<Map<String, Object>> allBreakdowns = new ArrayList<>();
        DiscoveryNodes nodes = clusterService().state().nodes();
        for (DiscoveryNode discoveryNode : nodes) {
            if (discoveryNode.isDataNode()) {
                Object actual = executeAnalysis.get(discoveryNode.getId());
                if (actual instanceof Map<?, ?> timings) {
                    Object qb = timings.get("QueryBreakdown");
                    if (qb instanceof List<?> list) {
                        for (Object item : list) {
                            allBreakdowns.add((Map<String, Object>) item);
                        }
                    }
                }
            }
        }
        return allBreakdowns;
    }

    @UseRandomizedOptimizerRules(0)
    @SuppressWarnings("unchecked")
    @Test
    public void test_explain_analyze_count_basic_reports_shard_timing() {
        execute("EXPLAIN ANALYZE SELECT COUNT(*) FROM locations");
        Map<String, Object> analysis = (Map<String, Object>) response.rows()[0][0];
        Map<String, Object> executeAnalysis = (Map<String, Object>) analysis.get("Execute");

        assertThat(executeAnalysis).containsKeys("Phases", "Total");
        Map<String, Map<String, Object>> phases = (Map<String, Map<String, Object>>) executeAnalysis.get("Phases");
        assertThat(phases).containsKey("0-count");

        List<Map<String, Object>> breakdowns = collectAllQueryBreakdowns(executeAnalysis);
        assertThat(breakdowns).isNotEmpty();

        for (Map<String, Object> breakdown : breakdowns) {
            assertThat(breakdown.get("SchemaName")).isEqualTo(sqlExecutor.getCurrentSchema());
            assertThat(breakdown.get("TableName")).isEqualTo("locations");
            assertThat(breakdown.get("ShardId")).isInstanceOf(Integer.class);
            assertThat((int) breakdown.get("ShardId")).isGreaterThanOrEqualTo(0);
            assertThat(breakdown.get("QueryName")).isEqualTo("CountQuery");
            assertThat(breakdown.get("QueryDescription")).isEqualTo("*:*");
            assertThat(breakdown.get("Time")).isInstanceOf(Double.class);
            assertThat((double) breakdown.get("Time")).isGreaterThanOrEqualTo(0.0);
            assertThat(breakdown).doesNotContainKey("PartitionIdent");
            assertThat(breakdown).doesNotContainKey("BreakDown");
            assertThat(breakdown).doesNotContainKey("Children");
        }
    }

    @UseRandomizedOptimizerRules(0)
    @SuppressWarnings("unchecked")
    @Test
    public void test_explain_analyze_count_with_where_clause() {
        execute("EXPLAIN ANALYZE SELECT COUNT(*) FROM locations WHERE name LIKE '%Ripple%' AND position > 0");
        Map<String, Object> analysis = (Map<String, Object>) response.rows()[0][0];
        Map<String, Object> executeAnalysis = (Map<String, Object>) analysis.get("Execute");

        List<Map<String, Object>> breakdowns = collectAllQueryBreakdowns(executeAnalysis);
        assertThat(breakdowns).isNotEmpty();

        for (Map<String, Object> breakdown : breakdowns) {
            assertThat(breakdown.get("SchemaName")).isEqualTo(sqlExecutor.getCurrentSchema());
            assertThat(breakdown.get("TableName")).isEqualTo("locations");
            assertThat(breakdown.get("QueryName")).isEqualTo("CountQuery");
            assertThat(breakdown.get("QueryDescription").toString()).contains("Ripple");
            assertThat(breakdown.get("Time")).isInstanceOf(Double.class);
            assertThat((double) breakdown.get("Time")).isGreaterThanOrEqualTo(0.0);
        }
    }

    @UseRandomizedOptimizerRules(0)
    @SuppressWarnings("unchecked")
    @Test
    public void test_explain_analyze_count_on_partitioned_table() {
        execute("CREATE TABLE my_schema.parted_count (id int, p int) PARTITIONED BY (p) with (number_of_replicas = 0)");
        execute("INSERT INTO my_schema.parted_count (id, p) VALUES (1, 10), (2, 20)");
        execute("REFRESH TABLE my_schema.parted_count");

        execute("EXPLAIN ANALYZE SELECT COUNT(*) FROM my_schema.parted_count");
        Map<String, Object> analysis = (Map<String, Object>) response.rows()[0][0];
        Map<String, Object> executeAnalysis = (Map<String, Object>) analysis.get("Execute");

        List<Map<String, Object>> breakdowns = collectAllQueryBreakdowns(executeAnalysis);
        assertThat(breakdowns).isNotEmpty();

        List<String> partitionIdents = new ArrayList<>();
        for (Map<String, Object> breakdown : breakdowns) {
            assertThat(breakdown.get("SchemaName")).isEqualTo("my_schema");
            assertThat(breakdown.get("TableName")).isEqualTo("parted_count");
            assertThat(breakdown.get("QueryName")).isEqualTo("CountQuery");
            assertThat(breakdown.get("PartitionIdent")).isNotNull();
            partitionIdents.add((String) breakdown.get("PartitionIdent"));
        }
        assertThat(partitionIdents).hasSizeGreaterThanOrEqualTo(2);
    }

    @UseRandomizedOptimizerRules(0)
    @SuppressWarnings("unchecked")
    @Test
    public void test_explain_analyze_count_partition_pruning() {
        execute("CREATE TABLE my_schema.parted_pruning (id int, p int) PARTITIONED BY (p) with (number_of_replicas = 0)");
        execute("INSERT INTO my_schema.parted_pruning (id, p) VALUES (1, 10), (2, 20)");
        execute("REFRESH TABLE my_schema.parted_pruning");

        execute("EXPLAIN ANALYZE SELECT COUNT(*) FROM my_schema.parted_pruning WHERE p = 10");
        Map<String, Object> analysis = (Map<String, Object>) response.rows()[0][0];
        Map<String, Object> executeAnalysis = (Map<String, Object>) analysis.get("Execute");

        List<Map<String, Object>> breakdowns = collectAllQueryBreakdowns(executeAnalysis);
        assertThat(breakdowns).isNotEmpty();

        List<String> partitionIdents = new ArrayList<>();
        for (Map<String, Object> breakdown : breakdowns) {
            assertThat(breakdown.get("SchemaName")).isEqualTo("my_schema");
            assertThat(breakdown.get("TableName")).isEqualTo("parted_pruning");
            partitionIdents.add((String) breakdown.get("PartitionIdent"));
        }

        execute("EXPLAIN ANALYZE SELECT COUNT(*) FROM my_schema.parted_pruning WHERE p = 20");
        Map<String, Object> analysis2 = (Map<String, Object>) response.rows()[0][0];
        Map<String, Object> executeAnalysis2 = (Map<String, Object>) analysis2.get("Execute");
        List<Map<String, Object>> breakdowns2 = collectAllQueryBreakdowns(executeAnalysis2);
        assertThat(breakdowns2).isNotEmpty();

        List<String> partitionIdents2 = new ArrayList<>();
        for (Map<String, Object> breakdown : breakdowns2) {
            partitionIdents2.add((String) breakdown.get("PartitionIdent"));
        }

        assertThat(partitionIdents).doesNotContainAnyElementsOf(partitionIdents2);
    }

    @UseRandomizedOptimizerRules(0)
    @SuppressWarnings("unchecked")
    @Test
    public void test_explain_analyze_count_routing_predicate() {
        execute("CREATE TABLE doc.routed_count (id int, name string) CLUSTERED BY (name) INTO 2 SHARDS WITH (number_of_replicas = 0)");
        execute("INSERT INTO doc.routed_count (id, name) VALUES (1, 'foo'), (2, 'bar')");
        execute("REFRESH TABLE doc.routed_count");

        execute("EXPLAIN ANALYZE SELECT COUNT(*) FROM doc.routed_count WHERE name = 'foo'");
        Map<String, Object> analysis = (Map<String, Object>) response.rows()[0][0];
        Map<String, Object> executeAnalysis = (Map<String, Object>) analysis.get("Execute");

        List<Map<String, Object>> breakdowns = collectAllQueryBreakdowns(executeAnalysis);

        assertThat(breakdowns).isNotEmpty();
        assertThat(breakdowns.getFirst().get("TableName")).isEqualTo("routed_count");
        assertThat(breakdowns.getFirst().get("QueryName")).isEqualTo("CountQuery");
    }

    @UseRandomizedOptimizerRules(0)
    @SuppressWarnings("unchecked")
    @Test
    public void test_explain_analyze_count_multi_node_execution() {
        execute("EXPLAIN ANALYZE SELECT COUNT(*) FROM locations");
        Map<String, Object> analysis = (Map<String, Object>) response.rows()[0][0];
        Map<String, Object> executeAnalysis = (Map<String, Object>) analysis.get("Execute");

        DiscoveryNodes nodes = clusterService().state().nodes();
        int nodesWithBreakdown = 0;
        List<Integer> shardIds = new ArrayList<>();
        for (DiscoveryNode discoveryNode : nodes) {
            if (discoveryNode.isDataNode()) {
                Object actual = executeAnalysis.get(discoveryNode.getId());
                assertThat(actual).isInstanceOf(Map.class);
                Map<String, Object> timings = (Map<String, Object>) actual;
                if (timings.containsKey("QueryBreakdown")) {
                    nodesWithBreakdown++;
                    List<Map<String, Object>> qbList = (List<Map<String, Object>>) timings.get("QueryBreakdown");
                    assertThat(qbList).isNotEmpty();
                    for (Map<String, Object> qb : qbList) {
                        shardIds.add((Integer) qb.get("ShardId"));
                    }
                }
            }
        }
        assertThat(nodesWithBreakdown).isEqualTo(2);
        assertThat(shardIds).containsExactlyInAnyOrder(0, 1);
    }

    @UseRandomizedOptimizerRules(0)
    @SuppressWarnings("unchecked")
    @Test
    public void test_explain_analyze_count_empty_result() {
        execute("EXPLAIN ANALYZE SELECT COUNT(*) FROM locations WHERE name = 'nonexistent_location'");
        Map<String, Object> analysis = (Map<String, Object>) response.rows()[0][0];
        Map<String, Object> executeAnalysis = (Map<String, Object>) analysis.get("Execute");
        List<Map<String, Object>> breakdowns = collectAllQueryBreakdowns(executeAnalysis);
        assertThat(breakdowns).isNotEmpty();
        for (Map<String, Object> qb : breakdowns) {
            assertThat(qb.get("QueryName")).isEqualTo("CountQuery");
            assertThat(qb.get("QueryDescription").toString()).contains("nonexistent_location");
            assertThat(qb.get("Time")).isNotNull();
        }

        execute("CREATE TABLE doc.empty_table (x int) clustered into 2 shards with (number_of_replicas = 0)");
        execute("EXPLAIN ANALYZE SELECT COUNT(*) FROM doc.empty_table");
        Map<String, Object> analysis2 = (Map<String, Object>) response.rows()[0][0];
        Map<String, Object> executeAnalysis2 = (Map<String, Object>) analysis2.get("Execute");
        List<Map<String, Object>> breakdowns2 = collectAllQueryBreakdowns(executeAnalysis2);
        assertThat(breakdowns2).hasSize(2);
        for (Map<String, Object> qb : breakdowns2) {
            assertThat(qb.get("TableName")).isEqualTo("empty_table");
            assertThat(qb.get("QueryName")).isEqualTo("CountQuery");
            assertThat(qb.get("QueryDescription")).isEqualTo("*:*");
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test_explain_analyze_regular_select_profiling_unchanged() {
        execute("EXPLAIN ANALYZE SELECT * FROM locations WHERE name = 'North West Ripple'");
        Map<String, Object> analysis = (Map<String, Object>) response.rows()[0][0];
        Map<String, Object> executeAnalysis = (Map<String, Object>) analysis.get("Execute");

        List<Map<String, Object>> breakdowns = collectAllQueryBreakdowns(executeAnalysis);
        assertThat(breakdowns).isNotEmpty();
        for (Map<String, Object> qb : breakdowns) {
            assertThat(qb.get("QueryName")).isNotEqualTo("CountQuery");
            assertThat(qb).containsKey("BreakDown");
        }
    }

    @Test
    public void test_count_without_explain_analyze_behavior_unchanged() {
        execute("SELECT COUNT(*) FROM locations");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat((long) response.rows()[0][0]).isEqualTo(13L);

        execute("SELECT COUNT(*) FROM locations WHERE name = 'North West Ripple'");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat((long) response.rows()[0][0]).isEqualTo(1L);
    }
}
