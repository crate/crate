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

package io.crate.analyze;

import static io.crate.metadata.FulltextAnalyzerResolver.CustomType.ANALYZER;
import static io.crate.testing.TestingHelpers.mapToSortedString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AutoExpandReplicas;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.junit.Before;
import org.junit.Test;

import io.crate.data.RowN;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.ddl.AlterTablePlan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.parser.ParsingException;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.TestingHelpers;

public class CreateAlterPartitionedTableAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private PlannerContext plannerContext;

    @Before
    public void prepare() throws IOException {
        String analyzerSettings = FulltextAnalyzerResolver.encodeSettings(
            Settings.builder().put("search", "foobar").build()).utf8ToString();
        Metadata metadata = Metadata.builder()
            .persistentSettings(
                Settings.builder()
                    .put(ANALYZER.buildSettingName("ft_search"), analyzerSettings)
                    .build())
            .build();
        ClusterState state =
            ClusterState.builder(ClusterName.DEFAULT)
                .nodes(DiscoveryNodes.builder()
                           .add(new DiscoveryNode("n1", buildNewFakeTransportAddress(),
                                                  Version.CURRENT))
                           .add(new DiscoveryNode("n2", buildNewFakeTransportAddress(),
                                                  Version.CURRENT))
                           .add(new DiscoveryNode("n3", buildNewFakeTransportAddress(),
                                                  Version.CURRENT))
                           .localNodeId("n1")
                )
                .metadata(metadata)
                .build();
        ClusterServiceUtils.setState(clusterService, state);
        RelationName multiPartName = new RelationName("doc", "multi_parted");
        e = SQLExecutor.builder(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .addPartitionedTable(
                TableDefinitions.TEST_PARTITIONED_TABLE_DEFINITION,
                TableDefinitions.TEST_PARTITIONED_TABLE_PARTITIONS)
            .addPartitionedTable(
                "create table doc.multi_parted (" +
                "   id int," +
                "   date timestamp with time zone," +
                "   num long," +
                "   obj object as (name string)" +
                ") partitioned by (date, obj['name'])",
                new PartitionName(multiPartName, Arrays.asList("1395874800000", "0")).toString(),
                new PartitionName(multiPartName, Arrays.asList("1395961200000", "-100")).toString(),
                new PartitionName(multiPartName, Arrays.asList(null, "-100")).toString()
            )
            .build();
        plannerContext = e.getPlannerContext(clusterService.state());
    }

    @SuppressWarnings("unchecked")
    private <S> S analyze(String stmt, Object... arguments) {
        AnalyzedStatement analyzedStatement = e.analyze(stmt);
        if (analyzedStatement instanceof AnalyzedCreateTable analyzedCreateTable) {
            return (S) analyzedCreateTable.bind(
                new NumberOfShards(clusterService),
                e.fulltextAnalyzerResolver(),
                plannerContext.nodeContext(),
                plannerContext.transactionContext(),
                new RowN(arguments),
                SubQueryResults.EMPTY
            );
        } else if (analyzedStatement instanceof AnalyzedAlterTable) {
            return (S) AlterTablePlan.bind(
                (AnalyzedAlterTable) analyzedStatement,
                plannerContext.transactionContext(),
                plannerContext.nodeContext(),
                new RowN(arguments),
                SubQueryResults.EMPTY
            );
        } else {
            throw new AssertionError("Statement of type " + analyzedStatement.getClass() + " not supported");
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPartitionedBy() {
        BoundCreateTable analysis = analyze("create table my_table (" +
                                            "  id integer," +
                                            "  no_index string index off," +
                                            "  name string," +
                                            "  date timestamp with time zone" +
                                            ") partitioned by (name)");
        assertThat(analysis.partitionedBy()).containsExactly(List.of("name", "keyword"));

        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> mappingProperties = (Map<String, Object>) mapping.get("properties");

        // partitioned columns must be not indexed in mapping
        Map<String, Object> nameMapping = (Map<String, Object>) mappingProperties.get("name");
        assertThat(mapToSortedString(nameMapping)).isEqualTo("index=false, position=3, type=keyword");

        Map<String, Object> metaMapping = (Map<String, Object>) mapping.get("_meta");
        List<List<String>> partitionedByMeta = (List<List<String>>) metaMapping.get("partitioned_by");
        assertThat(analysis.isPartitioned()).isTrue();
        assertThat(partitionedByMeta).hasSize(1);
        assertThat(partitionedByMeta.get(0).get(0)).isEqualTo("name");
        assertThat(partitionedByMeta.get(0).get(1)).isEqualTo("keyword");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPartitionedByMultipleColumns() {
        BoundCreateTable analysis = analyze("create table my_table (" +
                                            "  name string," +
                                            "  date timestamp with time zone" +
                                            ") partitioned by (name, date)");
        assertThat(analysis.partitionedBy()).hasSize(2);
        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");
        assertThat(mapToSortedString(properties)).isEqualTo(
                   "date={format=epoch_millis||strict_date_optional_time, index=false, position=2, type=date}, " +
                   "name={index=false, position=1, type=keyword}");
        assertThat(analysis.partitionedBy().get(0)).containsExactly("name", "keyword");
        assertThat(analysis.partitionedBy().get(1)).containsExactly("date", "date");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPartitionedByNestedColumns() {
        BoundCreateTable analysis = analyze("create table my_table (" +
                                            "  id integer," +
                                            "  no_index string index off," +
                                            "  o object as (" +
                                            "    name string" +
                                            "  )," +
                                            "  date timestamp with time zone" +
                                            ") partitioned by (date, o['name'])");
        assertThat(analysis.partitionedBy()).hasSize(2);

        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> mappingProperties = (Map<String, Object>) mapping.get("properties");
        Map<String, Object> oMapping = (Map<String, Object>) mappingProperties.get("o");
        assertThat(mapToSortedString(oMapping)).isEqualTo(
            "dynamic=true, position=3, properties={name={index=false, position=4, type=keyword}}, type=object");

        assertThat(analysis.partitionedBy().get(0)).containsExactly("date", "date");
        assertThat(analysis.partitionedBy().get(1)).containsExactly("o.name", "keyword");
    }

    @Test
    public void testPartitionedByArrayNestedColumns() {
        assertThatThrownBy(() -> analyze(
            "create table my_table (" +
            "  a array(object as (" +
            "    name string" +
            "  ))," +
            "  date timestamp with time zone" +
            ") partitioned by (date, a['name'])"
        )).isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testPartitionedByArray() {
        assertThatThrownBy(() -> analyze(
            "create table my_table (" +
            "  a array(string)," +
            "  date timestamp with time zone" +
            ") partitioned by (a)"
        )).isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testPartitionedByInnerArray() {
        assertThatThrownBy(() ->
            analyze("create table my_table (" +
                    "  a object as (names array(string))," +
                    "  date timestamp with time zone" +
                    ") partitioned by (a['names'])"
        )).isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testPartitionedByObject() {
        assertThatThrownBy(() -> analyze(
            "create table my_table (" +
            "  a object as(name string)," +
            "  date timestamp with time zone" +
            ") partitioned by (a)"
        )).isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testPartitionedByInnerObject() {
        assertThatThrownBy(() -> analyze(
            "create table my_table (" +
            "  a object as(b object as(name string))," +
            "  date timestamp with time zone" +
            ") partitioned by (a['b'])"
        )).isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testPartitionByUnknownColumn() {
        assertThatThrownBy(() -> analyze("create table my_table (p string) partitioned by (a)"))
            .isExactlyInstanceOf(ColumnUnknownException.class);
    }

    @Test
    public void testPartitionedByNotPartOfPrimaryKey() {
        assertThatThrownBy(() -> analyze(
            "create table my_table (" +
            "  id1 integer," +
            "  id2 integer," +
            "  date timestamp with time zone," +
            "  primary key (id1, id2)" +
            ") partitioned by (id1, date)"
        )).isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPartitionedByPartOfPrimaryKey() {
        BoundCreateTable analysis = analyze("create table my_table (" +
                                            "  id1 integer," +
                                            "  id2 integer," +
                                            "  date timestamp with time zone," +
                                            "  primary key (id1, id2)" +
                                            ") partitioned by (id1)");
        assertThat(analysis.partitionedBy()).containsExactly(List.of("id1", "integer"));

        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> mappingProperties = (Map<String, Object>) mapping.get("properties");

        Map<String, Object> oMapping = (Map<String, Object>) mappingProperties.get("id1");
        assertThat(mapToSortedString(oMapping)).isEqualTo("index=false, position=1, type=integer");
        Map<?, ?> meta = (Map<?, ?>) mapping.get("_meta");
        List<List<String>> partitionedBy = (List<List<String>>) meta.get("partitioned_by");
        assertThat(partitionedBy).containsExactly(List.of("id1", "integer"));
    }

    @Test
    public void testPartitionedByIndexed() {
        assertThatThrownBy(() -> analyze(
            "create table my_table(" +
            "  name string index using fulltext," +
            "  no_index string index off," +
            "  stuff string," +
            "  o object as (s string)," +
            "  index ft using fulltext(stuff, o['s']) with (analyzer='snowball')" +
            ") partitioned by (name)"
        )).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot use column name with fulltext index in PARTITIONED BY clause");
    }

    @Test
    public void testPartitionedByCompoundIndex() {
        assertThatThrownBy(() -> analyze(
            "create table my_table(" +
            "  name string index using fulltext," +
            "  no_index string index off," +
            "  stuff string," +
            "  o object as (s string)," +
            "  index ft using fulltext(stuff, o['s']) with (analyzer='snowball')" +
            ") partitioned by (ft)"
        )).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot use column ft with fulltext index in PARTITIONED BY clause");
    }

    @Test
    public void testPartitionedByClusteredBy() {
        assertThatThrownBy(() -> analyze(
            "create table my_table (" +
            "  id integer," +
            "  name string" +
            ") partitioned by (id)" +
            "  clustered by (id) into 5 shards"
        )).isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testAlterPartitionedTable() {
        BoundAlterTable analysis = analyze(
            "alter table parted set (number_of_replicas='0-all')");
        assertThat(analysis.partitionName()).isNotPresent();
        assertThat(analysis.isPartitioned()).isTrue();
        assertThat(analysis.tableParameter().settings().get(AutoExpandReplicas.SETTING.getKey())).isEqualTo("0-all");
    }

    @Test
    public void testAlterPartitionedTablePartition() {
        BoundAlterTable analysis = analyze(
            "alter table parted partition (date=1395874800000) set (number_of_replicas='0-all')");
        assertThat(analysis.partitionName()).isPresent();
        assertThat(analysis.partitionName().get()).isEqualTo(new PartitionName(
            new RelationName("doc", "parted"), Collections.singletonList("1395874800000")));
        assertThat(analysis.tableParameter().settings().get(AutoExpandReplicas.SETTING.getKey())).isEqualTo("0-all");
    }

    @Test
    public void testAlterPartitionedTableNonExistentPartition() {
        assertThatThrownBy(() -> analyze("alter table parted partition (date='1970-01-01') set (number_of_replicas='0-all')"))
            .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testAlterPartitionedTableInvalidPartitionColumns() {
        assertThatThrownBy(() -> analyze("alter table parted partition (a=1) set (number_of_replicas='0-all')"))
            .isExactlyInstanceOf(IllegalArgumentException.class);

    }

    @Test
    public void testAlterPartitionedTableInvalidNumber() {
        assertThatThrownBy(() -> analyze("alter table multi_parted partition (date=1395874800000) set (number_of_replicas='0-all')"))
            .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testAlterNonPartitionedTableWithPartitionClause() {
        assertThatThrownBy(() -> analyze("alter table users partition (date='1970-01-01') reset (number_of_replicas)"))
            .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testAlterPartitionedTableShards() {
        BoundAlterTable analysis = analyze(
            "alter table parted set (number_of_shards=10)");
        assertThat(analysis.partitionName()).isNotPresent();
        assertThat(analysis.isPartitioned()).isTrue();
        assertThat(analysis.tableParameter().settings().get(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey())).isEqualTo("10");
    }

    @Test
    public void testAlterTablePartitionWithNumberOfShards() {
        BoundAlterTable analysis = analyze(
            "alter table parted partition (date=1395874800000) set (number_of_shards=1)");
        assertThat(analysis.partitionName()).isPresent();
        assertThat(analysis.isPartitioned()).isTrue();
        assertThat(analysis.tableParameter().settings().get(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey())).isEqualTo("1");
    }

    @Test
    public void testAlterTablePartitionResetShards() {
        BoundAlterTable analysis = analyze(
            "alter table parted partition (date=1395874800000) reset (number_of_shards)");
        assertThat(analysis.partitionName()).isPresent();
        assertThat(analysis.isPartitioned()).isTrue();
        assertThat(analysis.tableParameter().settings().get(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey())).isEqualTo("5");
    }

    @Test
    public void testAlterPartitionedTablePartitionColumnPolicy() {
        assertThatThrownBy(() -> analyze("alter table parted partition (date=1395874800000) set (column_policy='strict')"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid property \"column_policy\" passed to [ALTER | CREATE] TABLE statement");
    }

    @Test
    public void testAlterPartitionedTableOnlyWithPartition() {
        assertThatThrownBy(() -> analyze("alter table ONLY parted partition (date=1395874800000) set (column_policy='strict')"))
            .isExactlyInstanceOf(ParsingException.class);
    }

    @Test
    public void testCreatePartitionedByGeoShape() {
        assertThatThrownBy(() -> analyze("create table shaped (id int, shape geo_shape) partitioned by (shape)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot use column shape of type geo_shape in PARTITIONED BY clause");
    }

    @Test
    public void testAlterTableWithWaitForActiveShards() {
        BoundAlterTable analyzedStatement = analyze(
            "ALTER TABLE parted SET (\"write.wait_for_active_shards\"= 'ALL')");
        assertThat(analyzedStatement.tableParameter().settings().get(IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey()))
            .isEqualTo("ALL");

        analyzedStatement = analyze("ALTER TABLE parted RESET (\"write.wait_for_active_shards\")");
        assertThat(analyzedStatement.tableParameter().settings().get(IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey()))
            .isEqualTo("1");
    }
}
