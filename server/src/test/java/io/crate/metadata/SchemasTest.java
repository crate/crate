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

import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataUpgradeService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.expression.udf.UserDefinedFunctionMetadata;
import io.crate.expression.udf.UserDefinedFunctionsMetadata;
import io.crate.metadata.doc.DocSchemaInfoFactory;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.view.ViewsMetadata;
import io.crate.metadata.view.ViewsMetadataTest;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class SchemasTest extends CrateDummyClusterServiceUnitTest {

    private final NodeContext nodeCtx = createNodeContext();
    private MetadataUpgradeService metadataUpgradeService;

    @Before
    public void setUpUpgradeService() throws Exception {
        metadataUpgradeService = new MetadataUpgradeService(
            nodeCtx,
            new IndexScopedSettings(Settings.EMPTY, Set.of()),
            null
        );
    }

    @Test
    public void testSystemSchemaIsNotWritable() throws Exception {
        Roles roles = () -> List.of(Role.CRATE_USER);
        var sysSchemaInfo = new SysSchemaInfo(clusterService, roles);
        Map<String, SchemaInfo> builtInSchemas = Map.of("sys", sysSchemaInfo);
        DocSchemaInfoFactory docSchemaInfoFactory = mock(DocSchemaInfoFactory.class);
        try (Schemas schemas = new Schemas(builtInSchemas, clusterService, docSchemaInfoFactory, roles)) {
            QualifiedName qname = QualifiedName.of("sys", "summits");
            assertThatThrownBy(() -> schemas.findRelation(qname, Operation.INSERT, Role.CRATE_USER, SearchPath.pathWithPGCatalogAndDoc()))
                .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
                .hasMessage("The relation \"sys.summits\" doesn't support or allow INSERT operations");
        }
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
        assertThat(Schemas.getNewCurrentSchemas(metadata)).containsExactlyInAnyOrder("doc", "new_schema");
    }

    @Test
    public void testSchemasFromViews() {
        Metadata metadata = Metadata.builder()
            .putCustom(
                ViewsMetadata.TYPE,
                ViewsMetadataTest.createMetadata()
            ).build();
        assertThat(Schemas.getNewCurrentSchemas(metadata)).containsExactlyInAnyOrder("doc", "my_schema");
    }


    @Test
    public void testCurrentSchemas() throws Exception {
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder(UUIDs.randomBase64UUID())
                .state(IndexMetadata.State.OPEN)
                .settings(Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(SETTING_VERSION_CREATED, Version.CURRENT))
                .indexName("doc.d1")
                .build(), true)
            .put(IndexMetadata.builder(UUIDs.randomBase64UUID())
                .state(IndexMetadata.State.CLOSE)
                .settings(Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(SETTING_VERSION_CREATED, Version.CURRENT))
                .indexName("doc.d2")
                .build(), true)
            .put(IndexMetadata.builder(UUIDs.randomBase64UUID())
                .state(IndexMetadata.State.CLOSE)
                .settings(Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(SETTING_VERSION_CREATED, Version.CURRENT))
                .indexName("foo.f1")
                .build(), true)
            .put(IndexMetadata.builder(UUIDs.randomBase64UUID())
                .state(IndexMetadata.State.OPEN)
                .settings(Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(SETTING_VERSION_CREATED, Version.CURRENT))
                .indexName("foo.f2")
                .build(), true)
            .build();
        metadata = metadataUpgradeService.upgradeMetadata(metadata);
        assertThat(Schemas.getNewCurrentSchemas(metadata)).containsExactly("foo", "doc");
    }

    @Test
    public void testResolveTableInfoForValidFQN() throws IOException {
        RelationName tableIdent = RelationName.of(QualifiedName.of("crate", "schema", "t"), null);
        SQLExecutor sqlExecutor = getSqlExecutorBuilderForTable(tableIdent, "doc", "schema");

        QualifiedName fqn = QualifiedName.of("crate", "schema", "t");
        var sessionSettings = sqlExecutor.getSessionSettings();
        TableInfo tableInfo = sqlExecutor.schemas()
            .findRelation(fqn, Operation.READ, sessionSettings.sessionUser(), sessionSettings.searchPath());

        RelationName relation = tableInfo.ident();
        assertThat(relation.schema()).isEqualTo("schema");
        assertThat(relation.name()).isEqualTo("t");
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
        assertThatThrownBy(() ->
                sqlExecutor.schemas().findRelation(
                    invalidFqn,
                    Operation.READ,
                    sessionSetttings.sessionUser(),
                    sessionSetttings.searchPath()))
            .isExactlyInstanceOf(SchemaUnknownException.class)
            .hasMessage("Schema 'bogus_schema' unknown");
    }

    @Test
    public void testResolveTableInfoThrowsRelationUnknownIfRelationIsNotInSearchPath() throws IOException {
        SQLExecutor sqlExecutor = getSqlExecutorBuilderForTable(new RelationName("schema", "t"));
        QualifiedName table = QualifiedName.of("missing_table");

        var sessionSettings = sqlExecutor.getSessionSettings();
        assertThatThrownBy(() -> sqlExecutor.schemas().findRelation(
                table,
                Operation.READ,
                sessionSettings.sessionUser(),
                sessionSettings.searchPath()))
            .isExactlyInstanceOf(RelationUnknown.class)
            .hasMessage("Relation 'missing_table' unknown");
    }

    @Test
    public void testResolveTableInfoLooksUpRelationInSearchPath() throws IOException {
        SQLExecutor sqlExecutor = getSqlExecutorBuilderForTable(new RelationName("schema", "t"), "doc", "schema");
        QualifiedName tableQn = QualifiedName.of("t");
        var sessionSettings = sqlExecutor.getSessionSettings();
        TableInfo tableInfo = sqlExecutor.schemas()
            .findRelation(tableQn, Operation.READ, sessionSettings.sessionUser(), sessionSettings.searchPath());

        RelationName relation = tableInfo.ident();
        assertThat(relation.schema()).isEqualTo("schema");
        assertThat(relation.name()).isEqualTo("t");
    }
}
