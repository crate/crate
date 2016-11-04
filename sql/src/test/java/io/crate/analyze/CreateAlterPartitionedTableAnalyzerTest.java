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

package io.crate.analyze;

import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.PartitionName;
import io.crate.sql.parser.ParsingException;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.test.ClusterServiceUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.crate.testing.TestingHelpers.mapToSortedString;
import static org.hamcrest.Matchers.*;

public class CreateAlterPartitionedTableAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        String analyzerSettings = FulltextAnalyzerResolver.encodeSettings(
            Settings.builder().put("search", "foobar").build()).utf8ToString();
        MetaData metaData = MetaData.builder()
                                    .persistentSettings(
                                        Settings.builder()
                                                .put("crate.analysis.custom.analyzer.ft_search", analyzerSettings)
                                                .build())
                                    .build();
        ClusterState state =
            ClusterState.builder(ClusterName.DEFAULT)
                        .nodes(DiscoveryNodes.builder()
                                             .add(new DiscoveryNode("n1", LocalTransportAddress.buildUnique(),
                                                                    Version.CURRENT))
                                             .add(new DiscoveryNode("n2", LocalTransportAddress.buildUnique(),
                                                                    Version.CURRENT))
                                             .add(new DiscoveryNode("n3", LocalTransportAddress.buildUnique(),
                                                                    Version.CURRENT))
                                             .localNodeId("n1")
                              )
                        .metaData(metaData)
                        .build();
        ClusterServiceUtils.setState(clusterService, state);
        e = SQLExecutor.builder(clusterService).enableDefaultTables().build();
    }

    @Test
    public void testPartitionedBy() throws Exception {
        CreateTableAnalyzedStatement analysis = e.analyze("create table my_table (" +
                                                          "  id integer," +
                                                          "  no_index string index off," +
                                                          "  name string," +
                                                          "  date timestamp" +
                                                          ") partitioned by (name)");
        assertThat(analysis.partitionedBy().size(), is(1));
        assertThat(analysis.partitionedBy().get(0), contains("name", "string"));

        // partitioned columns must be not indexed in mapping
        Map<String, Object> nameMapping = (Map<String, Object>) analysis.mappingProperties().get("name");
        assertThat(mapToSortedString(nameMapping), is("index=false, type=keyword"));

        Map<String, Object> metaMapping = (Map) analysis.mapping().get("_meta");
        assertThat((Map<String, Object>) metaMapping.get("columns"), not(hasKey("name")));
        List<List<String>> partitionedByMeta = (List<List<String>>) metaMapping.get("partitioned_by");
        assertTrue(analysis.isPartitioned());
        assertThat(partitionedByMeta.size(), is(1));
        assertThat(partitionedByMeta.get(0).get(0), is("name"));
        assertThat(partitionedByMeta.get(0).get(1), is("string"));
    }

    @Test
    public void testPartitionedByMultipleColumns() throws Exception {
        CreateTableAnalyzedStatement analysis = e.analyze("create table my_table (" +
                                                          "  name string," +
                                                          "  date timestamp" +
                                                          ") partitioned by (name, date)");
        assertThat(analysis.partitionedBy().size(), is(2));
        Map<String, Object> properties = analysis.mappingProperties();
        assertThat(mapToSortedString(properties),
            is("date={format=epoch_millis||strict_date_optional_time, index=false, type=date}, " +
               "name={index=false, type=keyword}"));
        assertThat((Map<String, Object>) ((Map) analysis.mapping().get("_meta")).get("columns"),
            allOf(
                not(hasKey("name")),
                not(hasKey("date"))
            ));
        assertThat(analysis.partitionedBy().get(0), contains("name", "string"));
        assertThat(analysis.partitionedBy().get(1), contains("date", "date"));
    }

    @Test
    public void testPartitionedByNestedColumns() throws Exception {
        CreateTableAnalyzedStatement analysis = e.analyze("create table my_table (" +
                                                          "  id integer," +
                                                          "  no_index string index off," +
                                                          "  o object as (" +
                                                          "    name string" +
                                                          "  )," +
                                                          "  date timestamp" +
                                                          ") partitioned by (date, o['name'])");
        assertThat(analysis.partitionedBy().size(), is(2));
        Map<String, Object> oMapping = (Map<String, Object>) analysis.mappingProperties().get("o");
        assertThat(mapToSortedString(oMapping), is(
            "dynamic=true, properties={name={index=false, type=keyword}}, type=object"));
        assertThat((Map<String, Object>) ((Map) analysis.mapping().get("_meta")).get("columns"), not(hasKey("date")));

        Map metaColumns = (Map) ((Map) analysis.mapping().get("_meta")).get("columns");
        assertNull(metaColumns);
        assertThat(analysis.partitionedBy().get(0), contains("date", "date"));
        assertThat(analysis.partitionedBy().get(1), contains("o.name", "string"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionedByArrayNestedColumns() throws Exception {
        e.analyze("create table my_table (" +
                  "  a array(object as (" +
                  "    name string" +
                  "  ))," +
                  "  date timestamp" +
                  ") partitioned by (date, a['name'])");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionedByArray() throws Exception {
        e.analyze("create table my_table (" +
                  "  a array(string)," +
                  "  date timestamp" +
                  ") partitioned by (a)");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionedByInnerArray() throws Exception {
        e.analyze("create table my_table (" +
                  "  a object as (names array(string))," +
                  "  date timestamp" +
                  ") partitioned by (a['names'])");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionedByObject() throws Exception {
        e.analyze("create table my_table (" +
                  "  a object as(name string)," +
                  "  date timestamp" +
                  ") partitioned by (a)");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionedByInnerObject() throws Exception {
        e.analyze("create table my_table (" +
                  "  a object as(b object as(name string))," +
                  "  date timestamp" +
                  ") partitioned by (a['b'])");
    }

    @Test
    public void testPartitionByUnknownColumn() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        e.analyze("create table my_table (p string) partitioned by (a)");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionedByNotPartOfPrimaryKey() throws Exception {
        e.analyze("create table my_table (" +
                  "  id1 integer," +
                  "  id2 integer," +
                  "  date timestamp," +
                  "  primary key (id1, id2)" +
                  ") partitioned by (id1, date)");
    }

    @Test
    public void testPartitionedByPartOfPrimaryKey() throws Exception {
        CreateTableAnalyzedStatement analysis = e.analyze("create table my_table (" +
                                                          "  id1 integer," +
                                                          "  id2 integer," +
                                                          "  date timestamp," +
                                                          "  primary key (id1, id2)" +
                                                          ") partitioned by (id1)");
        assertThat(analysis.partitionedBy().size(), is(1));
        assertThat(analysis.partitionedBy().get(0), contains("id1", "integer"));

        Map<String, Object> oMapping = (Map<String, Object>) analysis.mappingProperties().get("id1");
        assertThat(mapToSortedString(oMapping), is("index=false, type=integer"));
        assertThat((Map<String, Object>) ((Map) analysis.mapping().get("_meta")).get("columns"),
            not(hasKey("id1")));
    }

    @Test
    public void testPartitionedByIndexed() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot use column name with fulltext index in PARTITIONED BY clause");
        e.analyze("create table my_table(" +
                  "  name string index using fulltext," +
                  "  no_index string index off," +
                  "  stuff string," +
                  "  o object as (s string)," +
                  "  index ft using fulltext(stuff, o['s']) with (analyzer='snowball')" +
                  ") partitioned by (name)");

    }

    @Test
    public void testPartitionedByCompoundIndex() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot use column ft with fulltext index in PARTITIONED BY clause");
        e.analyze("create table my_table(" +
                  "  name string index using fulltext," +
                  "  no_index string index off," +
                  "  stuff string," +
                  "  o object as (s string)," +
                  "  index ft using fulltext(stuff, o['s']) with (analyzer='snowball')" +
                  ") partitioned by (ft)");

    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionedByClusteredBy() throws Exception {
        e.analyze("create table my_table (" +
                  "  id integer," +
                  "  name string" +
                  ") partitioned by (id)" +
                  "  clustered by (id) into 5 shards");
    }

    @Test
    public void testAlterPartitionedTable() throws Exception {
        AlterTableAnalyzedStatement analysis = e.analyze(
            "alter table parted set (number_of_replicas='0-all')");
        assertThat(analysis.partitionName().isPresent(), is(false));
        assertThat(analysis.table().isPartitioned(), is(true));
        assertEquals("0-all", analysis.tableParameter().settings().get(TableParameterInfo.AUTO_EXPAND_REPLICAS));
    }

    @Test
    public void testAlterPartitionedTablePartition() throws Exception {
        AlterTableAnalyzedStatement analysis = e.analyze(
            "alter table parted partition (date=1395874800000) set (number_of_replicas='0-all')");
        assertThat(analysis.partitionName().isPresent(), is(true));
        assertThat(analysis.partitionName().get(), is(new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000")))));
        assertThat(analysis.table().tableParameterInfo(), instanceOf(PartitionedTableParameterInfo.class));
        PartitionedTableParameterInfo tableSettingsInfo = (PartitionedTableParameterInfo) analysis.table().tableParameterInfo();
        assertThat(tableSettingsInfo.partitionTableSettingsInfo(), instanceOf(TableParameterInfo.class));
        assertEquals("0-all", analysis.tableParameter().settings().get(TableParameterInfo.AUTO_EXPAND_REPLICAS));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAlterPartitionedTableNonExistentPartition() throws Exception {
        e.analyze("alter table parted partition (date='1970-01-01') set (number_of_replicas='0-all')");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAlterPartitionedTableInvalidPartitionColumns() throws Exception {
        e.analyze("alter table parted partition (a=1) set (number_of_replicas='0-all')");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAlterPartitionedTableInvalidNumber() throws Exception {
        e.analyze("alter table multi_parted partition (date=1395874800000) set (number_of_replicas='0-all')");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAlterTableWithPartitionClause() throws Exception {
        e.analyze("alter table users partition (date='1970-01-01') reset (number_of_replicas)");
    }

    @Test
    public void testAlterPartitionedTableShards() throws Exception {
        AlterTableAnalyzedStatement analysis = e.analyze(
            "alter table parted set (number_of_shards=10)");
        assertThat(analysis.partitionName().isPresent(), is(false));
        assertThat(analysis.table().isPartitioned(), is(true));
        assertThat(analysis.table().tableParameterInfo(), instanceOf(PartitionedTableParameterInfo.class));
        assertEquals("10", analysis.tableParameter().settings().get(TableParameterInfo.NUMBER_OF_SHARDS));

        PartitionedTableParameterInfo tableSettingsInfo = (PartitionedTableParameterInfo) analysis.table().tableParameterInfo();
        TableParameter tableParameter = new TableParameter(
            analysis.tableParameter().settings(),
            tableSettingsInfo.partitionTableSettingsInfo().supportedInternalSettings());
        assertEquals(null, tableParameter.settings().get(TableParameterInfo.NUMBER_OF_SHARDS));

    }

    @Test
    public void testAlterPartitionedTablePartitionColumnPolicy() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid property \"column_policy\" passed to [ALTER | CREATE] TABLE statement");
        e.analyze("alter table parted partition (date=1395874800000) set (column_policy='strict')");
    }

    @Test
    public void testAlterPartitionedTableOnlyWithPartition() throws Exception {
        expectedException.expect(ParsingException.class);
        e.analyze("alter table ONLY parted partition (date=1395874800000) set (column_policy='strict')");
    }

    @Test
    public void testCreatePartitionedByGeoShape() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot use column shape of type geo_shape in PARTITIONED BY clause");
        e.analyze("create table shaped (id int, shape geo_shape) partitioned by (shape)");
    }
}
