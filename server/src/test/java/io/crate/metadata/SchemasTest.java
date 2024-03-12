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

package io.crate.metadata;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.expression.udf.UserDefinedFunctionMetadata;
import io.crate.expression.udf.UserDefinedFunctionsMetadata;
import io.crate.metadata.doc.DocSchemaInfoFactory;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.view.ViewsMetadata;
import io.crate.metadata.view.ViewsMetadataTest;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class SchemasTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testSystemSchemaIsNotWritable() throws Exception {
        expectedException.expect(OperationOnInaccessibleRelationException.class);
        expectedException.expectMessage("The relation \"foo.bar\" doesn't support or allow INSERT " +
                                        "operations, as it is read-only.");

        RelationName relationName = new RelationName("foo", "bar");
        SchemaInfo schemaInfo = mock(SchemaInfo.class);
        TableInfo tableInfo = mock(TableInfo.class);
        when(tableInfo.ident()).thenReturn(relationName);
        when(tableInfo.supportedOperations()).thenReturn(Operation.SYS_READ_ONLY);
        when(schemaInfo.getTableInfo(relationName.name())).thenReturn(tableInfo);
        when(schemaInfo.name()).thenReturn(relationName.schema());

        Schemas schemas = getReferenceInfos(schemaInfo);
        schemas.getTableInfo(relationName, Operation.INSERT);
    }

    @Test
    public void testSchemasFromUDF() {
        Metadata metadata = Metadata.builder()
            .putCustom(
                UserDefinedFunctionsMetadata.TYPE,
                UserDefinedFunctionsMetadata.of(
                    new UserDefinedFunctionMetadata("new_schema", "my_function", List.of(), DataTypes.STRING,
                                                    "burlesque", "Hello, World!Q")
                )
            ).build();
        assertThat(Schemas.getNewCurrentSchemas(metadata), containsInAnyOrder("doc", "new_schema"));
    }

    @Test
    public void testSchemasFromViews() {
        Metadata metadata = Metadata.builder()
            .putCustom(
                ViewsMetadata.TYPE,
                ViewsMetadataTest.createMetadata()
            ).build();
        assertThat(Schemas.getNewCurrentSchemas(metadata), containsInAnyOrder("doc", "my_schema"));
    }


    @Test
    public void testCurrentSchemas() throws Exception {
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("doc.d1")
                .state(IndexMetadata.State.OPEN)
                .settings(Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(SETTING_VERSION_CREATED, Version.CURRENT))
                .build(), true)
            .put(IndexMetadata.builder("doc.d2")
                .state(IndexMetadata.State.CLOSE)
                .settings(Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(SETTING_VERSION_CREATED, Version.CURRENT))
                .build(), true)
            .put(IndexMetadata.builder("foo.f1")
                .state(IndexMetadata.State.CLOSE)
                .settings(Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(SETTING_VERSION_CREATED, Version.CURRENT))
                .build(), true)
            .put(IndexMetadata.builder("foo.f2")
                .state(IndexMetadata.State.OPEN)
                .settings(Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(SETTING_VERSION_CREATED, Version.CURRENT))
                .build(), true)
            .build();
        assertThat(Schemas.getNewCurrentSchemas(metadata), contains("foo", "doc"));
    }

    private Schemas getReferenceInfos(SchemaInfo schemaInfo) {
        Map<String, SchemaInfo> builtInSchema = new HashMap<>();
        builtInSchema.put(schemaInfo.name(), schemaInfo);
        return new Schemas(builtInSchema, clusterService, mock(DocSchemaInfoFactory.class), List::of);
    }

    @Test
    public void testResolveTableInfoForValidFQN() throws IOException {
        RelationName tableIdent = RelationName.of(QualifiedName.of("crate", "schema", "t"), null);
        SQLExecutor sqlExecutor = getSqlExecutorBuilderForTable(tableIdent, "doc", "schema");

        QualifiedName fqn = QualifiedName.of("crate", "schema", "t");
        var sessionSettings = sqlExecutor.getSessionSettings();
        TableInfo tableInfo = sqlExecutor.schemas()
            .resolveTableInfo(fqn, Operation.READ, sessionSettings.sessionUser(), sessionSettings.searchPath());

        RelationName relation = tableInfo.ident();
        assertThat(relation.schema(), is("schema"));
        assertThat(relation.name(), is("t"));
    }

    private SQLExecutor getSqlExecutorBuilderForTable(RelationName tableIdent, String... searchPath) throws IOException {
        return SQLExecutor.of(clusterService)
            .setSearchPath(searchPath)
            .addTable("create table " + tableIdent.fqn() + " (id int)");
    }

    @Test
    public void testResolveTableInfoForInvalidFQNThrowsSchemaUnknownException() throws IOException {
        SQLExecutor sqlExecutor = getSqlExecutorBuilderForTable(new RelationName("schema", "t"));
        QualifiedName invalidFqn = QualifiedName.of("bogus_schema", "t");

        var sessionSetttings = sqlExecutor.getSessionSettings();
        expectedException.expect(SchemaUnknownException.class);
        expectedException.expectMessage("Schema 'bogus_schema' unknown");
        sqlExecutor.schemas().resolveTableInfo(invalidFqn, Operation.READ, sessionSetttings.sessionUser(), sessionSetttings.searchPath());
    }

    @Test
    public void testResolveTableInfoThrowsRelationUnknownIfRelationIsNotInSearchPath() throws IOException {
        SQLExecutor sqlExecutor = getSqlExecutorBuilderForTable(new RelationName("schema", "t"));
        QualifiedName table = QualifiedName.of("missing_table");

        var sessionSettings = sqlExecutor.getSessionSettings();
        expectedException.expect(RelationUnknown.class);
        expectedException.expectMessage("Relation 'missing_table' unknown");
        sqlExecutor.schemas().resolveTableInfo(table, Operation.READ, sessionSettings.sessionUser(), sessionSettings.searchPath());
    }

    @Test
    public void testResolveTableInfoLooksUpRelationInSearchPath() throws IOException {
        SQLExecutor sqlExecutor = getSqlExecutorBuilderForTable(new RelationName("schema", "t"), "doc", "schema");
        QualifiedName tableQn = QualifiedName.of("t");
        var sessionSettings = sqlExecutor.getSessionSettings();
        TableInfo tableInfo = sqlExecutor.schemas()
            .resolveTableInfo(tableQn, Operation.READ, sessionSettings.sessionUser(), sessionSettings.searchPath());

        RelationName relation = tableInfo.ident();
        assertThat(relation.schema(), is("schema"));
        assertThat(relation.name(), is("t"));
    }

    @Test
    public void testResolveRelationThrowsRelationUnknownfForInvalidFQN() throws IOException {
        SQLExecutor sqlExecutor = getSqlExecutorBuilderForTable(new RelationName("schema", "t"), "schema");
        QualifiedName invalidFqn = QualifiedName.of("bogus_schema", "t");

        expectedException.expect(RelationUnknown.class);
        expectedException.expectMessage("Relation 'bogus_schema.t' unknown");
        sqlExecutor.schemas().resolveRelation(invalidFqn, sqlExecutor.getSessionSettings().searchPath());
    }

    @Test
    public void testResolveRelationThrowsRelationUnknownIfRelationIsNotInSearchPath() throws IOException {
        SQLExecutor sqlExecutor = getSqlExecutorBuilderForTable(new RelationName("schema", "t"), "doc", "schema");
        QualifiedName table = QualifiedName.of("missing_table");

        expectedException.expect(RelationUnknown.class);
        expectedException.expectMessage("Relation 'missing_table' unknown");
        sqlExecutor.schemas().resolveRelation(table, sqlExecutor.getSessionSettings().searchPath());
    }

    @Test
    public void testResolveRelationForTableAndView() throws IOException {
        SQLExecutor sqlExecutor = getSqlExecutorBuilderForTable(new RelationName("schema", "t"), "doc", "schema")
            .addView(new RelationName("schema", "view"), "select 1");

        QualifiedName table = QualifiedName.of("t");
        RelationName tableRelation = sqlExecutor.schemas().resolveRelation(table, sqlExecutor.getSessionSettings().searchPath());
        assertThat(tableRelation.schema(), is("schema"));
        assertThat(tableRelation.name(), is("t"));

        QualifiedName view = QualifiedName.of("view");
        RelationName viewRelation = sqlExecutor.schemas().resolveRelation(view, sqlExecutor.getSessionSettings().searchPath());
        assertThat(viewRelation.schema(), is("schema"));
        assertThat(viewRelation.name(), is("view"));
    }
}
