/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.replication.logical.action;

import static io.crate.replication.logical.LogicalReplicationSettings.REPLICATION_INDEX_ROUTING_ACTIVE;
import static io.crate.role.metadata.RolesHelper.userOf;
import static io.crate.testing.TestingHelpers.createNodeContext;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataUpgradeService;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.test.MockLogAppender;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.replication.logical.metadata.Publication;
import io.crate.replication.logical.metadata.RelationMetadata;
import io.crate.role.Permission;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.role.Securable;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class PublicationsStateActionTest extends CrateDummyClusterServiceUnitTest {

    private MockLogAppender appender;

    @Before
    public void appendLogger() throws Exception {
        appender = new MockLogAppender();
        appender.start();
        Loggers.addAppender(LogManager.getLogger(Publication.class), appender);

    }

    @After
    public void removeLogger() {
        Loggers.removeAppender(LogManager.getLogger(Publication.class), appender);
        appender.stop();
    }

    @Test
    public void test_resolve_relation_names_for_all_tables_ignores_table_when_pub_owner_doesnt_have_read_write_define_permissions() throws Exception {
        var publicationOwner = userOf("publisher");
        var subscriber = userOf("subscriber");

        Roles roles = new Roles() {
            @Override
            public Collection<Role> roles() {
                return List.of(publicationOwner, subscriber);
            }

            @Override
            public boolean hasPrivilege(Role user, Permission permission, Securable securable, @Nullable String ident) {
                if (user.name().equals("publisher")) {
                    return "doc.t1".equals(ident);
                } else {
                    return true;
                }
            }
        };

        SQLExecutor.of(clusterService)
            .addTable("CREATE TABLE doc.t1 (id int)")
            .addTable("CREATE TABLE doc.t3 (id int)")
            .startShards("doc.t1", "doc.t3");
        var publication = new Publication("publisher", true, List.of());

        Metadata.Builder metadataBuilder = Metadata.builder();
        publication.resolveCurrentRelations(
            clusterService.state(),
            roles,
            publicationOwner,
            subscriber,
            "dummy",
            metadataBuilder
        );
        Metadata metadata = metadataBuilder.build();

        List<RelationName> relationNames = metadata.relations(org.elasticsearch.cluster.metadata.RelationMetadata.Table.class).stream()
            .map(org.elasticsearch.cluster.metadata.RelationMetadata.Table::name)
            .toList();
        assertThat(relationNames).contains(new RelationName("doc", "t1"));
    }

    @Test
    public void test_resolve_relation_names_for_all_tables_ignores_table_when_subscriber_doesnt_have_read_permissions() throws Exception {
        var publicationOwner = userOf("publisher");
        var subscriber = userOf("subscriber");

        Roles roles = new Roles() {
            @Override
            public Collection<Role> roles() {
                return List.of(publicationOwner, subscriber);
            }

            @Override
            public boolean hasPrivilege(Role user, Permission permission, Securable securable, @Nullable String ident) {
                if (user.name().equals("subscriber")) {
                    return "doc.t1".equals(ident);
                } else {
                    return true;
                }
            }

        };

        SQLExecutor.of(clusterService)
            .addTable("CREATE TABLE doc.t1 (id int)")
            .addTable("CREATE TABLE doc.t3 (id int)")
            .startShards("doc.t1", "doc.t2");
        var publication = new Publication("publisher", true, List.of());

        Metadata.Builder metadataBuilder = Metadata.builder();
        publication.resolveCurrentRelations(
            clusterService.state(),
            roles,
            publicationOwner,
            subscriber,
            "dummy",
            metadataBuilder
        );
        Metadata metadata = metadataBuilder.build();

        List<RelationName> relationNames = metadata.relations(org.elasticsearch.cluster.metadata.RelationMetadata.Table.class).stream()
            .map(org.elasticsearch.cluster.metadata.RelationMetadata.Table::name)
            .toList();
        assertThat(relationNames).contains(new RelationName("doc", "t1"));
    }

    @Test
    public void test_resolve_relation_names_for_fixed_tables_ignores_table_when_subscriber_doesnt_have_read_permissions() throws Exception {
        var publicationOwner = userOf("publisher");
        var subscriber = userOf("subscriber");

        Roles roles = new Roles() {
            @Override
            public Collection<Role> roles() {
                return List.of(publicationOwner, subscriber);
            }

            @Override
            public boolean hasPrivilege(Role user, Permission permission, Securable securable, @Nullable String ident) {
                if (user.name().equals("subscriber")) {
                    return "doc.t1".equals(ident);
                } else {
                    return true;
                }
            }
        };

        SQLExecutor.of(clusterService)
            .addTable("CREATE TABLE doc.t1 (id int)")
            .addTable("CREATE TABLE doc.t2 (id int)")
            .startShards("doc.t1", "doc.t2");
        var publication = new Publication("publisher", false,
            List.of(
                RelationName.of(QualifiedName.of("t1"), Schemas.DOC_SCHEMA_NAME),
                RelationName.of(QualifiedName.of("t2"), Schemas.DOC_SCHEMA_NAME)
            )
        );

        Metadata.Builder metadataBuilder = Metadata.builder();
        publication.resolveCurrentRelations(
            clusterService.state(),
            roles,
            publicationOwner,
            subscriber,
            "dummy",
            metadataBuilder
        );
        Metadata metadata = metadataBuilder.build();

        List<RelationName> relationNames = metadata.relations(org.elasticsearch.cluster.metadata.RelationMetadata.Table.class).stream()
            .map(org.elasticsearch.cluster.metadata.RelationMetadata.Table::name)
            .toList();
        assertThat(relationNames).contains(new RelationName("doc", "t1"));
    }

    @Test
    public void test_resolve_relation_names_for_all_tables_ignores_table_with_non_active_primary_shards() throws Exception {
        var user = userOf("dummy");
        Roles roles = new Roles() {
            @Override
            public Collection<Role> roles() {
                return List.of(user);
            }

            @Override
            public boolean hasPrivilege(Role user, Permission permission, Securable securable, @Nullable String ident) {
                return true; // This test case doesn't check privileges.
            }
        };

        SQLExecutor.of(clusterService)
            .addTable("CREATE TABLE doc.t1 (id int)")
            .addTable("CREATE TABLE doc.t2 (id int)")
            .startShards("doc.t1");      // <- only t1 has active primary shards;
        var publication = new Publication("some_user", true, List.of());

        Metadata.Builder metadataBuilder = Metadata.builder();
        publication.resolveCurrentRelations(
            clusterService.state(),
            roles,
            user,
            user,
            "dummy",
            metadataBuilder
        );
        Metadata metadata = metadataBuilder.build();

        List<RelationName> relationNames = metadata.relations(org.elasticsearch.cluster.metadata.RelationMetadata.Table.class).stream()
            .map(org.elasticsearch.cluster.metadata.RelationMetadata.Table::name)
            .toList();

        assertThat(relationNames).contains(new RelationName("doc", "t1"));
    }

    @Test
    public void test_resolve_relation_names_for_concrete_tables_marks_table_with_non_active_primary_shards() throws Exception {
        var user = userOf("dummy");
        Roles roles = new Roles() {
            @Override
            public Collection<Role> roles() {
                return List.of(user);
            }

            @Override
            public boolean hasPrivilege(Role user, Permission permission, Securable securable, @Nullable String ident) {
                return true; // This test case doesn't check privileges.
            }
        };

        SQLExecutor.of(clusterService)
            .addTable("CREATE TABLE doc.t1 (id int)")
            .addTable("CREATE TABLE doc.t2 (id int)")
            .startShards("doc.t1");      // <- only t1 has active primary shards;
        var publication = new Publication(
            "some_user",
            false,
            List.of(RelationName.fromIndexName("t1"), RelationName.fromIndexName("doc.t2"))
        );

        Metadata.Builder metadataBuilder = Metadata.builder();
        publication.resolveCurrentRelations(
            clusterService.state(),
            roles,
            user,
            user,
            "dummy",
            metadataBuilder
        );
        Metadata metadata = metadataBuilder.build();

        List<IndexMetadata> indices_t1 = metadata.getIndices(new RelationName("doc", "t1"), List.of(), true, im -> im);
        IndexMetadata indexMetadata_t1 = indices_t1.getFirst();
        assertThat(REPLICATION_INDEX_ROUTING_ACTIVE.get(indexMetadata_t1.getSettings())).isTrue();

        List<IndexMetadata> indices_t2 = metadata.getIndices(new RelationName("doc", "t2"), List.of(), true, im -> im);
        IndexMetadata indexMetadata_t2 = indices_t2.getFirst();
        assertThat(REPLICATION_INDEX_ROUTING_ACTIVE.get(indexMetadata_t2.getSettings())).isFalse();
    }

    @Test
    public void test_resolve_relation_names_for_all_tables_marks_partition_with_non_active_primary_shards() throws Exception {
        var user = userOf("dummy");
        Roles roles = new Roles() {
            @Override
            public Collection<Role> roles() {
                return List.of(user);
            }

            @Override
            public boolean hasPrivilege(Role user, Permission permission, Securable securable, @Nullable String ident) {
                return true; // This test case doesn't check privileges.
            }
        };

        SQLExecutor.of(clusterService)
            .addTable(
                "CREATE TABLE doc.p1 (id int, p int) partitioned by (p)",
                new PartitionName(new RelationName("doc", "p1"), singletonList("1")).asIndexName()
            );
        var publication = new Publication("some_user", true, List.of());

        Metadata.Builder metadataBuilder = Metadata.builder();
        publication.resolveCurrentRelations(
            clusterService.state(),
            roles,
            user,
            user,
            "dummy",
            metadataBuilder
        );
        Metadata metadata = metadataBuilder.build();

        List<IndexMetadata> indices = metadata.getIndices(new RelationName("doc", "p1"), List.of(), true, im -> im);
        IndexMetadata indexMetadata = indices.getFirst();
        assertThat(REPLICATION_INDEX_ROUTING_ACTIVE.get(indexMetadata.getSettings())).isFalse();
    }

    @Test
    public void test_resolve_relation_names_for_concrete_tables_marks_partition_with_non_active_primary_shards() throws Exception {
        var user = userOf("dummy");
        Roles roles = new Roles() {
            @Override
            public Collection<Role> roles() {
                return List.of(user);
            }

            @Override
            public boolean hasPrivilege(Role user, Permission permission, Securable securable, @Nullable String ident) {
                return true; // This test case doesn't check privileges.
            }
        };

        SQLExecutor.of(clusterService)
            .addTable(
                "CREATE TABLE doc.p1 (id int, p int) partitioned by (p)",
                new PartitionName(new RelationName("doc", "p1"), singletonList("1")).asIndexName()
            );
        var publication = new Publication(
            "some_user",
            false,
            List.of(RelationName.fromIndexName("p1"))
        );

        Metadata.Builder metadataBuilder = Metadata.builder();
        publication.resolveCurrentRelations(
            clusterService.state(),
            roles,
            user,
            user,
            "dummy",
            metadataBuilder
        );
        Metadata metadata = metadataBuilder.build();

        List<IndexMetadata> indices = metadata.getIndices(new RelationName("doc", "p1"), List.of(), true, im -> im);
        IndexMetadata indexMetadata = indices.getFirst();
        assertThat(REPLICATION_INDEX_ROUTING_ACTIVE.get(indexMetadata.getSettings())).isFalse();
    }

    @Test
    public void test_bwc_streaming_5() throws Exception {
        var publicationOwner = userOf("publisher");
        var subscriber = userOf("subscriber");

        Roles roles = new Roles() {
            @Override
            public Collection<Role> roles() {
                return List.of(publicationOwner, subscriber);
            }

            @Override
            public boolean hasPrivilege(Role user, Permission permission, Securable securable, @Nullable String ident) {
                return true;
            }

        };

        RelationName relationName1 = new RelationName("doc", "t1");
        RelationName relationName2 = new RelationName("doc", "t2");
        PartitionName partitionName = new PartitionName(relationName2, singletonList("1"));

        SQLExecutor.of(clusterService)
            .addTable("CREATE TABLE doc.t1 (id int)")
            .addTable("CREATE TABLE doc.t2 (id int, p int) PARTITIONED BY (p)", partitionName.asIndexName())
            .startShards("doc.t1", "doc.t2");
        var publication = new Publication("publisher", true, List.of());

        Metadata.Builder metadataBuilder = Metadata.builder();
        publication.resolveCurrentRelations(
            clusterService.state(),
            roles,
            publicationOwner,
            subscriber,
            "dummy",
            metadataBuilder
        );
        Metadata metadata = metadataBuilder.build();
        PublicationsStateAction.Response response = new PublicationsStateAction.Response(metadata, List.of());

        MetadataUpgradeService metadataUpgradeService = new MetadataUpgradeService(createNodeContext(), IndexScopedSettings.DEFAULT_SCOPED_SETTINGS, null);

        // Ensure a node < 6.0.0 can read the response
        {
            BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(Version.V_5_10_0);
            response.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                in.setVersion(Version.V_5_10_0);
                Map<RelationName, RelationMetadata> relationsInPublications = in.readMap(RelationName::new, RelationMetadata::new);
                assertThat(relationsInPublications).hasSize(2);
                assertThat(relationsInPublications.keySet()).contains(relationName1, relationName2);

                RelationMetadata relationMetadataT1 = relationsInPublications.get(relationName1);
                assertThat(relationMetadataT1.indices().stream().map(im -> im.getIndex().getName()).toList()).containsExactly(relationName1.indexNameOrAlias());
                assertThat(relationMetadataT1.template()).isNull();

                RelationMetadata relationMetadataT2 = relationsInPublications.get(relationName2);
                assertThat(relationMetadataT2.indices().stream().map(im -> im.getIndex().getName()).toList()).containsExactly(partitionName.asIndexName());
                assertThat(relationMetadataT2.template()).isNotNull();
            }
        }

        // Ensure a response from a node < 6.0.0 can be read/converted
        {
            BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(Version.V_5_10_0);
            response.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                in.setVersion(Version.V_5_10_0);
                PublicationsStateAction.Response response1 = new PublicationsStateAction.Response(in);
                Metadata metadata1 = metadataUpgradeService.upgradeMetadata(response1.metadata());
                org.elasticsearch.cluster.metadata.RelationMetadata.Table table1 = metadata1.getRelation(relationName1);
                assertThat(table1).isNotNull();
                org.elasticsearch.cluster.metadata.RelationMetadata.Table table2 = metadata1.getRelation(relationName2);
                assertThat(table2).isNotNull();
                assertThat(table2.partitionedBy()).containsExactly(ColumnIdent.of("p"));
            }
        }
    }
}
