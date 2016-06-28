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
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.Operation;
import io.crate.sql.SqlFormatter;
import io.crate.sql.tree.CreateTable;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.mockito.Mockito.mock;

public class MetaDataToASTNodeResolverTest extends CrateUnitTest {

    class TestDocTableInfo extends DocTableInfo {

        public TestDocTableInfo(TableIdent ident,
                                int numberOfShards,
                                String numberOfReplicas,
                                List<ReferenceInfo> columns,
                                List<ReferenceInfo> partitionedByColumns,
                                List<GeneratedReferenceInfo> generatedColumns,
                                ImmutableMap<ColumnIdent, IndexReferenceInfo> indexColumns,
                                ImmutableMap<ColumnIdent, ReferenceInfo> references,
                                ImmutableMap<ColumnIdent, String> analyzers,
                                List<ColumnIdent> primaryKeys,
                                List<ColumnIdent> notNullColumns,
                                ColumnIdent clusteredBy,
                                ImmutableMap<String, Object> tableParameters,
                                List<ColumnIdent> partitionedBy,
                                ColumnPolicy policy) {
            super(ident,
                columns,
                partitionedByColumns,
                generatedColumns,
                indexColumns,
                references,
                analyzers,
                primaryKeys,
                clusteredBy,
                false, false,
                new String[]{},
                mock(ClusterService.class),
                new IndexNameExpressionResolver(Settings.EMPTY),
                numberOfShards,
                new BytesRef(numberOfReplicas),
                tableParameters,
                partitionedBy,
                Collections.EMPTY_LIST,
                policy,
                Operation.ALL,
                mock(ExecutorService.class));
        }
    }

    private static ReferenceInfo newReferenceInfo(TableIdent tableIdent, String name, DataType type) {
        return newReferenceInfo(tableIdent, name, type, null, null, false);
    }

    private static ReferenceInfo newReferenceInfo(TableIdent tableIdent,
                                                  String name,
                                                  DataType type,
                                                  @Nullable List<String> path,
                                                  @Nullable ColumnPolicy policy,
                                                  Boolean partitionColumn) {
        return new ReferenceInfo(
                new ReferenceIdent(tableIdent, name, path),
                partitionColumn ? RowGranularity.PARTITION : RowGranularity.DOC,
                type,
                policy == null ? ColumnPolicy.DYNAMIC : policy,
                ReferenceInfo.IndexType.NOT_ANALYZED, false);
    }

    private static ImmutableMap<ColumnIdent, ReferenceInfo> referencesMap(List<ReferenceInfo> columns) {
        ImmutableMap.Builder<ColumnIdent, ReferenceInfo> referencesMap = ImmutableMap.builder();
        for (ReferenceInfo info : columns) {
            referencesMap.put(info.ident().columnIdent(), info);
        }
        return referencesMap.build();
    }


    @Test
    public void testBuildCreateTableColumns() throws Exception {
        TableIdent ident = new TableIdent("doc", "test");

        List<ReferenceInfo> columns = ImmutableList.of(
                newReferenceInfo(ident, "bools", DataTypes.BOOLEAN),
                newReferenceInfo(ident, "bytes", DataTypes.BYTE),
                newReferenceInfo(ident, "strings", DataTypes.STRING),
                newReferenceInfo(ident, "shorts", DataTypes.SHORT),
                newReferenceInfo(ident, "floats", DataTypes.FLOAT),
                newReferenceInfo(ident, "doubles", DataTypes.DOUBLE),
                newReferenceInfo(ident, "ints", DataTypes.INTEGER),
                newReferenceInfo(ident, "longs", DataTypes.LONG),
                newReferenceInfo(ident, "timestamp", DataTypes.TIMESTAMP),
                newReferenceInfo(ident, "ip_addr", DataTypes.IP),
                newReferenceInfo(ident, "arr_simple", new ArrayType(DataTypes.STRING)),
                newReferenceInfo(ident, "arr_geo_point", new ArrayType(DataTypes.GEO_POINT)),
                newReferenceInfo(ident, "arr_obj", new ArrayType(DataTypes.OBJECT), null, ColumnPolicy.STRICT, false),
                newReferenceInfo(ident, "arr_obj", DataTypes.LONG, Arrays.asList("col_1"), null, false),
                newReferenceInfo(ident, "arr_obj", DataTypes.STRING, Arrays.asList("col_2"), null, false),
                newReferenceInfo(ident, "obj", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC, false),
                newReferenceInfo(ident, "obj", DataTypes.LONG, Arrays.asList("col_1"), null, false),
                newReferenceInfo(ident, "obj", DataTypes.STRING, Arrays.asList("col_2"), null, false)
        );

        DocTableInfo tableInfo = new TestDocTableInfo(
                ident,
                5, "0-all",
                columns,
                ImmutableList.<ReferenceInfo>of(),
                ImmutableList.<GeneratedReferenceInfo>of(),
                ImmutableMap.<ColumnIdent, IndexReferenceInfo>of(),
                referencesMap(columns),
                ImmutableMap.<ColumnIdent, String>of(),
                ImmutableList.<ColumnIdent>of(),
                ImmutableList.<ColumnIdent>of(),
                null,
                ImmutableMap.<String, Object>of(),
                ImmutableList.<ColumnIdent>of(),
                ColumnPolicy.DYNAMIC);

        CreateTable node = MetaDataToASTNodeResolver.resolveCreateTable(tableInfo);
        assertEquals("CREATE TABLE IF NOT EXISTS \"doc\".\"test\" (\n" +
                "   \"bools\" BOOLEAN,\n" +
                "   \"bytes\" BYTE,\n" +
                "   \"strings\" STRING,\n" +
                "   \"shorts\" SHORT,\n" +
                "   \"floats\" FLOAT,\n" +
                "   \"doubles\" DOUBLE,\n" +
                "   \"ints\" INTEGER,\n" +
                "   \"longs\" LONG,\n" +
                "   \"timestamp\" TIMESTAMP,\n" +
                "   \"ip_addr\" IP,\n" +
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
                "CLUSTERED INTO 5 SHARDS\n" +
                "WITH (\n" +
                "   column_policy = 'dynamic',\n" +
                "   number_of_replicas = '0-all'\n" +
                ")",
                SqlFormatter.formatSql(node));
    }

    @Test
    public void testBuildCreateTablePrimaryKey() throws Exception {
        TableIdent ident = new TableIdent("myschema", "test");

        List<ReferenceInfo> columns = ImmutableList.of(
                newReferenceInfo(ident, "pk_col_one", DataTypes.LONG),
                newReferenceInfo(ident, "pk_col_two", DataTypes.LONG)
        );
        List<ColumnIdent> primaryKeys = ImmutableList.of(
                new ColumnIdent("pk_col_one"),
                new ColumnIdent("pk_col_two")
        );

        DocTableInfo tableInfo = new TestDocTableInfo(
                ident,
                5, "0-all",
                columns,
                ImmutableList.<ReferenceInfo>of(),
                ImmutableList.<GeneratedReferenceInfo>of(),
                ImmutableMap.<ColumnIdent, IndexReferenceInfo>of(),
                referencesMap(columns),
                ImmutableMap.<ColumnIdent, String>of(),
                primaryKeys,
                ImmutableList.<ColumnIdent>of(),
                null,
                ImmutableMap.<String, Object>of(),
                ImmutableList.<ColumnIdent>of(),
                ColumnPolicy.STRICT);

        CreateTable node = MetaDataToASTNodeResolver.resolveCreateTable(tableInfo);
        assertEquals("CREATE TABLE IF NOT EXISTS \"myschema\".\"test\" (\n" +
                "   \"pk_col_one\" LONG,\n" +
                "   \"pk_col_two\" LONG,\n" +
                "   PRIMARY KEY (\"pk_col_one\", \"pk_col_two\")\n" +
                ")\n" +
                "CLUSTERED INTO 5 SHARDS\n" +
                "WITH (\n" +
                "   column_policy = 'strict',\n" +
                "   number_of_replicas = '0-all'\n" +
                ")",
                SqlFormatter.formatSql(node));
    }

    @Test
    public void testBuildCreateTableParameters() throws Exception {
        TableIdent ident = new TableIdent("myschema", "test");

        List<ReferenceInfo> columns = ImmutableList.of(
                newReferenceInfo(ident, "id", DataTypes.LONG)
        );
        ImmutableMap.Builder<String, Object> tableParameters = ImmutableMap.builder();
        tableParameters.put("refresh_interval", 10000L)
                .put("param_array", new String[]{"foo", "bar"})
                .put("param_obj", new HashMap<String, Object>() {{
                    put("foo", "bar");
                    put("int", 42);
                }})
                .put("index.translog.flush_interval", 100L);

        DocTableInfo tableInfo = new TestDocTableInfo(
                ident,
                5, "5",
                columns,
                ImmutableList.<ReferenceInfo>of(),
                ImmutableList.<GeneratedReferenceInfo>of(),
                ImmutableMap.<ColumnIdent, IndexReferenceInfo>of(),
                referencesMap(columns),
                ImmutableMap.<ColumnIdent, String>of(),
                ImmutableList.<ColumnIdent>of(),
                ImmutableList.<ColumnIdent>of(),
                null,
                tableParameters.build(),
                ImmutableList.<ColumnIdent>of(),
                ColumnPolicy.IGNORED);

        CreateTable node = MetaDataToASTNodeResolver.resolveCreateTable(tableInfo);
        assertEquals("CREATE TABLE IF NOT EXISTS \"myschema\".\"test\" (\n" +
                "   \"id\" LONG\n" +
                ")\n" +
                "CLUSTERED INTO 5 SHARDS\n" +
                "WITH (\n" +
                "   column_policy = 'ignored',\n" +
                "   \"index.translog.flush_interval\" = 100,\n" +
                "   number_of_replicas = '5',\n" +
                "   param_array = ['foo','bar'],\n" +
                "   param_obj = {\"foo\"= 'bar', \"int\"= 42},\n" +
                "   refresh_interval = 10000\n" +
                ")",
                SqlFormatter.formatSql(node));
    }

    @Test
    public void testBuildCreateTableClusteredByPartitionedBy() throws Exception {
        TableIdent ident = new TableIdent("myschema", "test");

        List<ReferenceInfo> columns = ImmutableList.of(
                newReferenceInfo(ident, "id", DataTypes.LONG),
                newReferenceInfo(ident, "partition_column", DataTypes.STRING, null, null, true),
                newReferenceInfo(ident, "cluster_column", DataTypes.STRING)
        );

        DocTableInfo tableInfo = new TestDocTableInfo(
                ident,
                5, "0-all",
                columns,
                ImmutableList.of(columns.get(1)),
                ImmutableList.<GeneratedReferenceInfo>of(),
                ImmutableMap.<ColumnIdent, IndexReferenceInfo>of(),
                referencesMap(columns),
                ImmutableMap.<ColumnIdent, String>of(),
                ImmutableList.<ColumnIdent>of(),
                ImmutableList.<ColumnIdent>of(),
                new ColumnIdent("cluster_column"),
                ImmutableMap.<String, Object>of(),
                ImmutableList.of(columns.get(1).ident().columnIdent()),
                ColumnPolicy.DYNAMIC);

        CreateTable node = MetaDataToASTNodeResolver.resolveCreateTable(tableInfo);
        assertEquals("CREATE TABLE IF NOT EXISTS \"myschema\".\"test\" (\n" +
                "   \"id\" LONG,\n" +
                "   \"partition_column\" STRING,\n" +
                "   \"cluster_column\" STRING\n" +
                ")\n" +
                "CLUSTERED BY (\"cluster_column\") INTO 5 SHARDS\n" +
                "PARTITIONED BY (\"partition_column\")\n" +
                "WITH (\n" +
                "   column_policy = 'dynamic',\n" +
                "   number_of_replicas = '0-all'\n" +
                ")",
                SqlFormatter.formatSql(node));
    }


    @Test
    public void testBuildCreateTableIndexes() throws Exception {
        TableIdent ident = new TableIdent("myschema", "test");
        ReferenceInfo colA = new ReferenceInfo(new ReferenceIdent(ident, "col_a", null),
                RowGranularity.DOC, DataTypes.STRING, null, ReferenceInfo.IndexType.NOT_ANALYZED, false);
        ReferenceInfo colB = new ReferenceInfo(new ReferenceIdent(ident, "col_b", null),
                RowGranularity.DOC, DataTypes.STRING, null, ReferenceInfo.IndexType.ANALYZED, false);
        ReferenceInfo colC = new ReferenceInfo(new ReferenceIdent(ident, "col_c", null),
                RowGranularity.DOC, DataTypes.STRING, null, ReferenceInfo.IndexType.NO, false);
        ReferenceInfo colD = new ReferenceInfo(new ReferenceIdent(ident, "col_d", null),
                RowGranularity.DOC, DataTypes.OBJECT);
        ReferenceInfo colE = new ReferenceInfo(new ReferenceIdent(ident, "col_d", Arrays.asList("a")),
                RowGranularity.DOC, DataTypes.STRING, null, ReferenceInfo.IndexType.NOT_ANALYZED, false);

        List<ReferenceInfo> columns = ImmutableList.of(
                newReferenceInfo(ident, "id", DataTypes.LONG),
                colA, colB, colC, colD, colE
        );

        ImmutableMap.Builder<ColumnIdent, IndexReferenceInfo> indexBuilder = ImmutableMap.builder();
        indexBuilder
                .put(new ColumnIdent("col_a_col_b_ft"),
                        new IndexReferenceInfo(new ReferenceIdent(ident, "col_a_col_b_ft"), ReferenceInfo.IndexType.ANALYZED,
                                ImmutableList.of(colA, colB), "english"))
                .put(new ColumnIdent("col_d_a_ft"),
                        new IndexReferenceInfo(new ReferenceIdent(ident, "col_d_a_ft"), ReferenceInfo.IndexType.ANALYZED,
                                ImmutableList.of(colE), "custom_analyzer"));

        DocTableInfo tableInfo = new TestDocTableInfo(
                ident,
                5, "0-all",
                columns,
                ImmutableList.<ReferenceInfo>of(),
                ImmutableList.<GeneratedReferenceInfo>of(),
                indexBuilder.build(),
                referencesMap(columns),
                ImmutableMap.<ColumnIdent, String>of(),
                ImmutableList.<ColumnIdent>of(),
                ImmutableList.<ColumnIdent>of(),
                null,
                ImmutableMap.<String, Object>of(),
                ImmutableList.<ColumnIdent>of(),
                ColumnPolicy.DYNAMIC);


        CreateTable node = MetaDataToASTNodeResolver.resolveCreateTable(tableInfo);
        assertEquals("CREATE TABLE IF NOT EXISTS \"myschema\".\"test\" (\n" +
                "   \"id\" LONG,\n" +
                "   \"col_a\" STRING,\n" +
                "   \"col_b\" STRING INDEX USING FULLTEXT,\n" +
                "   \"col_c\" STRING INDEX OFF,\n" +
                "   \"col_d\" OBJECT (DYNAMIC) AS (\n" +
                "      \"a\" STRING\n" +
                "   ),\n" +
                "   INDEX \"col_a_col_b_ft\" USING FULLTEXT (\"col_a\", \"col_b\") WITH (\n" +
                "      analyzer = 'english'\n" +
                "   ),\n" +
                "   INDEX \"col_d_a_ft\" USING FULLTEXT (\"col_d\"['a']) WITH (\n" +
                "      analyzer = 'custom_analyzer'\n" +
                "   )\n" +
                ")\n" +
                "CLUSTERED INTO 5 SHARDS\n" +
                "WITH (\n" +
                "   column_policy = 'dynamic',\n" +
                "   number_of_replicas = '0-all'\n" +
                ")",
                SqlFormatter.formatSql(node));
    }
}
