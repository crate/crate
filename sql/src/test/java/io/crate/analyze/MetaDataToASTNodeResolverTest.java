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
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.IndexReference;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.Operation;
import io.crate.sql.SqlFormatter;
import io.crate.sql.tree.CreateTable;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static java.util.Arrays.asList;

public class MetaDataToASTNodeResolverTest extends CrateUnitTest {

    class TestDocTableInfo extends DocTableInfo {

        TestDocTableInfo(RelationName ident,
                         int numberOfShards,
                         String numberOfReplicas,
                         List<Reference> columns,
                         List<Reference> partitionedByColumns,
                         List<GeneratedReference> generatedColumns,
                         ImmutableMap<ColumnIdent, IndexReference> indexColumns,
                         ImmutableMap<ColumnIdent, Reference> references,
                         ImmutableMap<ColumnIdent, String> analyzers,
                         List<ColumnIdent> primaryKeys,
                         ColumnIdent clusteredBy,
                         ImmutableMap<String, Object> tableParameters,
                         List<ColumnIdent> partitionedBy,
                         ColumnPolicy policy) {
            super(ident,
                columns,
                partitionedByColumns,
                generatedColumns,
                ImmutableList.of(),
                indexColumns,
                references,
                analyzers,
                primaryKeys,
                clusteredBy,
                false,
                new String[0],
                new String[0],
                new IndexNameExpressionResolver(Settings.EMPTY),
                numberOfShards,
                numberOfReplicas,
                tableParameters,
                partitionedBy,
                ImmutableList.of(),
                policy,
                Version.CURRENT,
                null,
                false,
                Operation.ALL);
        }
    }

    private static Reference newReference(RelationName relationName, String name, DataType type) {
        return newReference(relationName, name, type, null, null, false, false);
    }

    private static Reference newReference(RelationName relationName,
                                          String name,
                                          DataType type,
                                          @Nullable List<String> path,
                                          @Nullable ColumnPolicy policy,
                                          Boolean partitionColumn,
                                          boolean columnStoreDisabled) {
        return new Reference(
            new ReferenceIdent(relationName, name, path),
            partitionColumn ? RowGranularity.PARTITION : RowGranularity.DOC,
            type,
            policy == null ? ColumnPolicy.DYNAMIC : policy,
            Reference.IndexType.NOT_ANALYZED, true, columnStoreDisabled);
    }

    private static ImmutableMap<ColumnIdent, Reference> referencesMap(List<Reference> columns) {
        ImmutableMap.Builder<ColumnIdent, Reference> referencesMap = ImmutableMap.builder();
        for (Reference info : columns) {
            referencesMap.put(info.column(), info);
        }
        return referencesMap.build();
    }


    @Test
    public void testBuildCreateTableColumns() throws Exception {
        RelationName ident = new RelationName("doc", "test");

        List<Reference> columns = ImmutableList.of(
            newReference(ident, "bools", DataTypes.BOOLEAN),
            newReference(ident, "bytes", DataTypes.BYTE),
            newReference(ident, "strings", DataTypes.STRING),
            newReference(ident, "shorts", DataTypes.SHORT),
            newReference(ident, "floats", DataTypes.FLOAT),
            newReference(ident, "doubles", DataTypes.DOUBLE),
            newReference(ident, "ints", DataTypes.INTEGER),
            newReference(ident, "longs", DataTypes.LONG),
            newReference(ident, "timestamp", DataTypes.TIMESTAMP),
            newReference(ident, "ip_addr", DataTypes.IP),
            newReference(ident, "arr_simple", new ArrayType(DataTypes.STRING)),
            newReference(ident, "arr_geo_point", new ArrayType(DataTypes.GEO_POINT)),
            newReference(ident, "arr_obj", new ArrayType(DataTypes.OBJECT), null, ColumnPolicy.STRICT, false, false),
            newReference(ident, "arr_obj", DataTypes.LONG, Collections.singletonList("col_1"), null, false, false),
            newReference(ident, "arr_obj", DataTypes.STRING, Collections.singletonList("col_2"), null, false, false),
            newReference(ident, "obj", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC, false, false),
            newReference(ident, "obj", DataTypes.LONG, Collections.singletonList("col_1"), null, false, false),
            newReference(ident, "obj", DataTypes.STRING, Collections.singletonList("col_2"), null, false, false)
        );

        DocTableInfo tableInfo = new TestDocTableInfo(
            ident,
            5, "0-all",
            columns,
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableMap.of(),
            referencesMap(columns),
            ImmutableMap.of(),
            ImmutableList.of(),
            null,
            ImmutableMap.of(),
            ImmutableList.of(),
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
        RelationName ident = new RelationName("myschema", "test");

        List<Reference> columns = ImmutableList.of(
            newReference(ident, "pk_col_one", DataTypes.LONG),
            newReference(ident, "pk_col_two", DataTypes.LONG)
        );
        List<ColumnIdent> primaryKeys = ImmutableList.of(
            new ColumnIdent("pk_col_one"),
            new ColumnIdent("pk_col_two")
        );

        DocTableInfo tableInfo = new TestDocTableInfo(
            ident,
            5, "0-all",
            columns,
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableMap.of(),
            referencesMap(columns),
            ImmutableMap.of(),
            primaryKeys,
            null,
            ImmutableMap.of(),
            ImmutableList.of(),
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
    public void testBuildCreateTableNotNull() throws Exception {
        RelationName ident = new RelationName("myschema", "test");

        Reference colA = new Reference(new ReferenceIdent(ident, "col_a", null),
            RowGranularity.DOC, DataTypes.STRING, null, Reference.IndexType.NOT_ANALYZED, true);
        Reference colB = new Reference(new ReferenceIdent(ident, "col_b", null),
            RowGranularity.DOC, DataTypes.STRING, null, Reference.IndexType.ANALYZED, false);
        List<Reference> columns = ImmutableList.of(colA, colB);

        List<ColumnIdent> primaryKeys = ImmutableList.of(new ColumnIdent("col_a"));

        DocTableInfo tableInfo = new TestDocTableInfo(
            ident,
            5, "0-all",
            columns,
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableMap.of(),
            referencesMap(columns),
            ImmutableMap.of(),
            primaryKeys,
            null,
            ImmutableMap.of(),
            ImmutableList.of(),
            ColumnPolicy.STRICT);

        CreateTable node = MetaDataToASTNodeResolver.resolveCreateTable(tableInfo);
        assertEquals("CREATE TABLE IF NOT EXISTS \"myschema\".\"test\" (\n" +
                     "   \"col_a\" STRING,\n" +
                     "   \"col_b\" STRING NOT NULL INDEX USING FULLTEXT,\n" +
                     "   PRIMARY KEY (\"col_a\")\n" +
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
        RelationName ident = new RelationName("myschema", "test");

        List<Reference> columns = ImmutableList.of(
            newReference(ident, "id", DataTypes.LONG)
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
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableMap.of(),
            referencesMap(columns),
            ImmutableMap.of(),
            ImmutableList.of(),
            null,
            tableParameters.build(),
            ImmutableList.of(),
            ColumnPolicy.IGNORED);

        CreateTable node = MetaDataToASTNodeResolver.resolveCreateTable(tableInfo);
        assertEquals("CREATE TABLE IF NOT EXISTS \"myschema\".\"test\" (\n" +
                     "   \"id\" LONG\n" +
                     ")\n" +
                     "CLUSTERED INTO 5 SHARDS\n" +
                     "WITH (\n" +
                     "   column_policy = 'ignored',\n" +
                     "   number_of_replicas = '5',\n" +
                     "   param_array = ['foo', 'bar'],\n" +
                     "   param_obj = {\"foo\"= 'bar', \"int\"= 42},\n" +
                     "   refresh_interval = 10000,\n" +
                     "   \"translog.flush_interval\" = 100\n" +
                     ")",
            SqlFormatter.formatSql(node));
    }

    @Test
    public void testBuildCreateTableClusteredByPartitionedBy() throws Exception {
        RelationName ident = new RelationName("myschema", "test");

        List<Reference> columns = ImmutableList.of(
            newReference(ident, "id", DataTypes.LONG),
            newReference(ident, "partition_column", DataTypes.STRING, null, null, true, false),
            newReference(ident, "cluster_column", DataTypes.STRING)
        );

        DocTableInfo tableInfo = new TestDocTableInfo(
            ident,
            5, "0-all",
            columns,
            ImmutableList.of(columns.get(1)),
            ImmutableList.of(),
            ImmutableMap.of(),
            referencesMap(columns),
            ImmutableMap.of(),
            ImmutableList.of(),
            new ColumnIdent("cluster_column"),
            ImmutableMap.of(),
            ImmutableList.of(columns.get(1).column()),
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
        RelationName ident = new RelationName("myschema", "test");
        Reference colA = new Reference(new ReferenceIdent(ident, "col_a", null),
            RowGranularity.DOC, DataTypes.STRING, null, Reference.IndexType.NOT_ANALYZED, true);
        Reference colB = new Reference(new ReferenceIdent(ident, "col_b", null),
            RowGranularity.DOC, DataTypes.STRING, null, Reference.IndexType.ANALYZED, true);
        Reference colC = new Reference(new ReferenceIdent(ident, "col_c", null),
            RowGranularity.DOC, DataTypes.STRING, null, Reference.IndexType.NO, true);
        Reference colD = new Reference(new ReferenceIdent(ident, "col_d", null),
            RowGranularity.DOC, DataTypes.OBJECT);
        Reference colE = new Reference(new ReferenceIdent(ident, "col_d", asList("a")),
            RowGranularity.DOC, DataTypes.STRING, null, Reference.IndexType.NOT_ANALYZED, true);

        List<Reference> columns = ImmutableList.of(
            newReference(ident, "id", DataTypes.LONG),
            colA, colB, colC, colD, colE
        );

        ImmutableMap.Builder<ColumnIdent, IndexReference> indexBuilder = ImmutableMap.builder();
        indexBuilder
            .put(new ColumnIdent("col_a_col_b_ft"),
                new IndexReference(new ReferenceIdent(ident, "col_a_col_b_ft"), Reference.IndexType.ANALYZED,
                    ImmutableList.of(colA, colB), "english"))
            .put(new ColumnIdent("col_d_a_ft"),
                new IndexReference(new ReferenceIdent(ident, "col_d_a_ft"), Reference.IndexType.ANALYZED,
                    ImmutableList.of(colE), "custom_analyzer"))
            .put(new ColumnIdent("col_a_col_b_plain"),
                new IndexReference(new ReferenceIdent(ident, "col_a_col_b_plain"), Reference.IndexType.NOT_ANALYZED,
                    ImmutableList.of(colA, colB), null));

        DocTableInfo tableInfo = new TestDocTableInfo(
            ident,
            5, "0-all",
            columns,
            ImmutableList.of(),
            ImmutableList.of(),
            indexBuilder.build(),
            referencesMap(columns),
            ImmutableMap.of(),
            ImmutableList.of(),
            null,
            ImmutableMap.of(),
            ImmutableList.of(),
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
                     "   ),\n" +
                     "   INDEX \"col_a_col_b_plain\" USING PLAIN (\"col_a\", \"col_b\")\n" +
                     ")\n" +
                     "CLUSTERED INTO 5 SHARDS\n" +
                     "WITH (\n" +
                     "   column_policy = 'dynamic',\n" +
                     "   number_of_replicas = '0-all'\n" +
                     ")",
            SqlFormatter.formatSql(node));
    }

    @Test
    public void testBuildCreateTableStorageDefinitions() throws Exception {
        RelationName ident = new RelationName("myschema", "test");

        List<Reference> columns = ImmutableList.of(
            newReference(ident, "s", DataTypes.STRING, null, null, false, true)
        );

        DocTableInfo tableInfo = new TestDocTableInfo(
            ident,
            5, "0-all",
            columns,
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableMap.of(),
            referencesMap(columns),
            ImmutableMap.of(),
            ImmutableList.of(),
            null,
            ImmutableMap.of(),
            ImmutableList.of(),
            ColumnPolicy.STRICT);

        CreateTable node = MetaDataToASTNodeResolver.resolveCreateTable(tableInfo);
        assertEquals("CREATE TABLE IF NOT EXISTS \"myschema\".\"test\" (\n" +
                     "   \"s\" STRING STORAGE WITH (\n" +
                     "      columnstore = false\n" +
                     "   )\n" +
                     ")\n" +
                     "CLUSTERED INTO 5 SHARDS\n" +
                     "WITH (\n" +
                     "   column_policy = 'strict',\n" +
                     "   number_of_replicas = '0-all'\n" +
                     ")",
            SqlFormatter.formatSql(node));
    }

}
