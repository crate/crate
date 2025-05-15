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

import static io.crate.role.metadata.RolesHelper.userOf;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.MockLogAppender;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

        var resolvedRelations = publication.resolveCurrentRelations(
            clusterService.state(),
            roles,
            publicationOwner,
            subscriber,
            "dummy"
        );

        assertThat(resolvedRelations.keySet()).contains(new RelationName("doc", "t1"));
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

        var resolvedRelations = publication.resolveCurrentRelations(clusterService.state(), roles, publicationOwner, subscriber, "dummy");
        assertThat(resolvedRelations.keySet()).contains(new RelationName("doc", "t1"));
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

        var resolvedRelations = publication.resolveCurrentRelations(
            clusterService.state(),
            roles,
            publicationOwner,
            subscriber,
            "dummy"
        );
        assertThat(resolvedRelations.keySet()).contains(new RelationName("doc", "t1"));
    }

    @Test
    public void test_resolve_relation_names_for_all_tables_contains_all_tables_with_different_flags() throws Exception {
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

        var resolvedRelations = publication.resolveCurrentRelations(
            clusterService.state(),
            roles,
            user,
            user,
            "dummy"
        );

        List<RelationMetadata.ReplicatedIndex> indices = resolvedRelations.values().stream()
            .flatMap(x -> x.indices().stream())
            .sorted(Comparator.comparing(ri -> ri.indexMetadata().getIndex().getName()))
            .toList();

        assertThat(indices).satisfiesExactly(
            ri -> {
                assertThat(ri.indexMetadata().getIndex().getName()).isEqualTo("t1");
                assertThat(ri.allPrimaryShardsActive()).isTrue();
            },
            ri -> {
                assertThat(ri.indexMetadata().getIndex().getName()).isEqualTo("t2");
                assertThat(ri.allPrimaryShardsActive()).isFalse();
            }
        );
    }

    @Test
    public void test_resolve_relation_names_for_concrete_tables_contains_all_tables_with_different_flags() throws Exception {
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

        var resolvedRelations = publication.resolveCurrentRelations(
            clusterService.state(),
            roles,
            user,
            user,
            "dummy"
        );

        List<RelationMetadata.ReplicatedIndex> indices = resolvedRelations.values().stream()
            .flatMap(x -> x.indices().stream())
            .sorted(Comparator.comparing(ri -> ri.indexMetadata().getIndex().getName()))
            .toList();

        assertThat(indices).satisfiesExactly(
            ri -> {
                assertThat(ri.indexMetadata().getIndex().getName()).isEqualTo("t1");
                assertThat(ri.allPrimaryShardsActive()).isTrue();
            },
            ri -> {
                assertThat(ri.indexMetadata().getIndex().getName()).isEqualTo("t2");
                assertThat(ri.allPrimaryShardsActive()).isFalse();
            }
        );
    }

    @Test
    public void test_resolve_relation_names_for_all_tables_contains_partition_with_non_active_primary_shards_with_false_flag() throws Exception {
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

        var resolvedRelations = publication.resolveCurrentRelations(
            clusterService.state(),
            roles,
            user,
            user,
            "dummy"
        );

        List<RelationMetadata.ReplicatedIndex> indices = resolvedRelations.values().stream()
            .flatMap(x -> x.indices().stream())
            .toList();

        assertThat(indices).hasSize(1);
        assertThat(indices.getFirst().indexMetadata().getIndex().getName()).isEqualTo(".partitioned.p1.04132");
        assertThat(indices.getFirst().allPrimaryShardsActive()).isFalse();
    }

    @Test
    public void test_resolve_relation_names_for_concrete_tables_contains_partition_with_non_active_primary_shards_with_false_flag() throws Exception {
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

        var resolvedRelations = publication.resolveCurrentRelations(
            clusterService.state(),
            roles,
            user,
            user,
            "dummy"
        );

        List<RelationMetadata.ReplicatedIndex> indices = resolvedRelations.values().stream()
            .flatMap(x -> x.indices().stream())
            .toList();

        assertThat(indices).hasSize(1);
        assertThat(indices.getFirst().indexMetadata().getIndex().getName()).isEqualTo(".partitioned.p1.04132");
        assertThat(indices.getFirst().allPrimaryShardsActive()).isFalse();
    }
}
