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

import com.google.common.collect.ImmutableList;
import io.crate.core.collections.TreeMapBuilder;
import io.crate.metadata.*;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.planner.RowGranularity;
import io.crate.sql.SqlFormatter;
import io.crate.sql.tree.CreateTable;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.*;

public class MetaDataToASTNodeResolverTest extends CrateUnitTest {

    static TableIdent TEST_DOC_TABLE_IDENT = new TableIdent(ReferenceInfos.DEFAULT_SCHEMA_NAME, "users");

    static Routing SHARD_ROUTING = new Routing(TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder()
            .put("crate1", TreeMapBuilder.<String, List<Integer>>newMapBuilder().put("t1", Arrays.asList(1, 2)).map())
            .put("crate2", TreeMapBuilder.<String, List<Integer>>newMapBuilder().put("t1", Arrays.asList(3, 4)).map())
            .map());

    static ReferenceInfo COL_TEXT = new ReferenceInfo(new ReferenceIdent(TEST_DOC_TABLE_IDENT, "text", null),
            RowGranularity.DOC, DataTypes.STRING, null, ReferenceInfo.IndexType.ANALYZED);

    static ReferenceInfo COL_NAME = new ReferenceInfo(new ReferenceIdent(TEST_DOC_TABLE_IDENT, "name", null),
            RowGranularity.DOC, DataTypes.STRING, null, ReferenceInfo.IndexType.NOT_ANALYZED);

    @Test
    public void testBuildCreateTableColumns() throws Exception {
        TableIdent ident = new TableIdent("doc", "test");
        TableInfo tableInfo = TestingTableInfo.builder(ident, RowGranularity.DOC, SHARD_ROUTING)
                .add("bytes", DataTypes.BYTE, null)
                .add("strings", DataTypes.STRING, null)
                .add("shorts", DataTypes.SHORT, null)
                .add("floats", DataTypes.FLOAT, null)
                .add("doubles", DataTypes.DOUBLE, null)
                .add("ints", DataTypes.INTEGER, null)
                .add("longs", DataTypes.LONG, null)
                .add("timestamp", DataTypes.TIMESTAMP, null)
                .add("arr_simple", new ArrayType(DataTypes.STRING), null)
                .add("arr_geo_point", new ArrayType(DataTypes.GEO_POINT), null)
                .add("arr_obj", new ArrayType(DataTypes.OBJECT), null, ColumnPolicy.STRICT)
                .add("arr_obj", DataTypes.LONG, Arrays.asList("col_1"))
                .add("arr_obj", DataTypes.STRING, Arrays.asList("col_2"))
                .add("obj", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
                .add("obj", DataTypes.LONG, Arrays.asList("col_1"))
                .add("obj", DataTypes.STRING, Arrays.asList("col_2"))
                .build();
        CreateTable node = MetaDataToASTNodeResolver.resolveCreateTable(tableInfo);
        assertEquals("CREATE TABLE IF NOT EXISTS \"test\" (\n" +
                "   \"bytes\" BYTE,\n" +
                "   \"strings\" STRING,\n" +
                "   \"shorts\" SHORT,\n" +
                "   \"floats\" FLOAT,\n" +
                "   \"doubles\" DOUBLE,\n" +
                "   \"ints\" INTEGER,\n" +
                "   \"longs\" LONG,\n" +
                "   \"timestamp\" TIMESTAMP,\n" +
                "   \"arr_simple\" ARRAY(STRING),\n" +
                "   \"arr_geo_point\" ARRAY(GEO_POINT),\n" +
                "   \"arr_obj\" ARRAY(OBJECT (STRICT) AS (\n" +
                "      \"col_1\" LONG,\n" +
                "      \"col_2\" STRING\n" +
                "   )),\n" +
                "   \"obj\" OBJECT (DYNAMIC) AS (\n" +
                "      \"col_1\" LONG,\n" +
                "      \"col_2\" STRING\n" +
                "   )\n" +
                ")\n" +
                "CLUSTERED INTO 1 SHARDS", SqlFormatter.formatSql(node));
    }

    @Test
    public void testBuildCreateTablePrimaryKey() throws Exception {
        TableIdent ident = new TableIdent("myschema", "test");
        TableInfo tableInfo = TestingTableInfo.builder(ident, RowGranularity.DOC, SHARD_ROUTING)
                .add("pk_col_one", DataTypes.LONG, null)
                .add("pk_col_two", DataTypes.LONG, null)
                .addPrimaryKey("pk_col_one")
                .addPrimaryKey("pk_col_two")
                .build();
        CreateTable node = MetaDataToASTNodeResolver.resolveCreateTable(tableInfo);
        assertEquals("CREATE TABLE IF NOT EXISTS \"myschema\".\"test\" (\n" +
                "   \"pk_col_one\" LONG,\n" +
                "   \"pk_col_two\" LONG,\n" +
                "   PRIMARY KEY (\"pk_col_one\", \"pk_col_two\")\n" +
                ")\n" +
                "CLUSTERED INTO 1 SHARDS", SqlFormatter.formatSql(node));
    }

    @Test
    public void testBuildCreateTableParameters() throws Exception {
        TableIdent ident = new TableIdent("myschema", "test");
        TableInfo tableInfo = TestingTableInfo.builder(ident, RowGranularity.DOC, SHARD_ROUTING)
                .add("id", DataTypes.LONG, null)
                .addParameter("refresh_interval", 10000L)
                .addParameter("param_array", new String[]{"foo", "bar"})
                .addParameter("param_obj", new HashMap() {{
                    put("foo", "bar");
                    put("int", 42);
                }})
                .numberOfReplicas("0-all")
                .build();
        CreateTable node = MetaDataToASTNodeResolver.resolveCreateTable(tableInfo);
        assertEquals("CREATE TABLE IF NOT EXISTS \"myschema\".\"test\" (\n" +
                "   \"id\" LONG\n" +
                ")\n" +
                "CLUSTERED INTO 1 SHARDS\n" +
                "WITH (\n" +
                "   number_of_replicas = '0-all',\n" +
                "   param_array = ['foo','bar'],\n" +
                "   param_obj = {foo: 'bar', int: 42},\n" +
                "   refresh_interval = 10000\n" +
                ")", SqlFormatter.formatSql(node));
    }

    @Test
    public void testBuildCreateTableClusteredByPartitionedBy() throws Exception {
        TableIdent ident = new TableIdent("myschema", "test");
        TableInfo tableInfo = TestingTableInfo.builder(ident, RowGranularity.DOC, SHARD_ROUTING)
                .add("id", DataTypes.LONG, null)
                .add("partition_column", DataTypes.STRING, null, true)
                .add("cluster_column", DataTypes.STRING, null)
                .clusteredBy("cluster_column")
                .build();
        CreateTable node = MetaDataToASTNodeResolver.resolveCreateTable(tableInfo);
        assertEquals("CREATE TABLE IF NOT EXISTS \"myschema\".\"test\" (\n" +
                "   \"id\" LONG,\n" +
                "   \"partition_column\" STRING,\n" +
                "   \"cluster_column\" STRING\n" +
                ")\n" +
                "CLUSTERED BY (\"cluster_column\") INTO 1 SHARDS\n" +
                "PARTITIONED BY (\"partition_column\")", SqlFormatter.formatSql(node));
    }

    @Test
    public void testBuildCreateTableIndexes() throws Exception {
        TableIdent ident = new TableIdent("myschema", "test");
        ReferenceInfo colA = new ReferenceInfo(new ReferenceIdent(ident, "col_a", null),
                RowGranularity.DOC, DataTypes.STRING, null, ReferenceInfo.IndexType.ANALYZED);
        ReferenceInfo colB = new ReferenceInfo(new ReferenceIdent(ident, "col_b", null),
                RowGranularity.DOC, DataTypes.STRING, null, ReferenceInfo.IndexType.ANALYZED);
        ReferenceInfo colC = new ReferenceInfo(new ReferenceIdent(ident, "col_c", null),
                RowGranularity.DOC, DataTypes.STRING, null, ReferenceInfo.IndexType.NO);
        ReferenceInfo colD = new ReferenceInfo(new ReferenceIdent(ident, "col_d", null),
                RowGranularity.DOC, DataTypes.OBJECT);
        ReferenceInfo colE = new ReferenceInfo(new ReferenceIdent(ident, "col_d", Arrays.asList("a")),
                RowGranularity.DOC, DataTypes.STRING, null, ReferenceInfo.IndexType.ANALYZED);

        TableInfo tableInfo = TestingTableInfo.builder(ident, RowGranularity.DOC, SHARD_ROUTING)
                .add("id", DataTypes.LONG, null)
                .add(colA)
                .add(colB)
                .add(colC)
                .add(colD)
                .add(colE)
                .addIndex(new ColumnIdent("col_a_plain"), ReferenceInfo.IndexType.NOT_ANALYZED,
                        ImmutableList.of(colA))
                .addIndex(new ColumnIdent("col_b_ft"), ReferenceInfo.IndexType.ANALYZED,
                        ImmutableList.of(colB))
                .addIndex(new ColumnIdent("col_a_col_b_ft"), ReferenceInfo.IndexType.ANALYZED,
                        ImmutableList.of(colA, colB), "english")
                .addIndex(new ColumnIdent("col_d_a_ft"), ReferenceInfo.IndexType.ANALYZED,
                        ImmutableList.of(colE), "custom_analyzer")
                .build();
        CreateTable node = MetaDataToASTNodeResolver.resolveCreateTable(tableInfo);
        assertEquals("CREATE TABLE IF NOT EXISTS \"myschema\".\"test\" (\n" +
                "   \"id\" LONG,\n" +
                "   \"col_a\" STRING,\n" +
                "   \"col_b\" STRING,\n" +
                "   \"col_c\" STRING INDEX OFF,\n" +
                "   \"col_d\" OBJECT (DYNAMIC) AS (\n" +
                "      \"a\" STRING\n" +
                "   ),\n" +
                "   INDEX \"col_a_plain\" USING PLAIN (\"col_a\"),\n" +
                "   INDEX \"col_b_ft\" USING FULLTEXT (\"col_b\"),\n" +
                "   INDEX \"col_a_col_b_ft\" USING FULLTEXT (\"col_a\", \"col_b\") WITH (\n" +
                "      analyzer = 'english'\n" +
                "   ),\n" +
                "   INDEX \"col_d_a_ft\" USING FULLTEXT (\"col_d\"['a']) WITH (\n" +
                "      analyzer = 'custom_analyzer'\n" +
                "   )\n" +
                ")\n" +
                "CLUSTERED INTO 1 SHARDS", SqlFormatter.formatSql(node));
    }
}