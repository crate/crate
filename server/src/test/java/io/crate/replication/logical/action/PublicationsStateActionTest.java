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

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Level;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.test.MockLogAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.replication.logical.metadata.Publication;
import io.crate.replication.logical.metadata.RelationMetadata;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.user.Privilege;
import io.crate.user.User;
import io.crate.user.UserLookup;

public class PublicationsStateActionTest extends CrateDummyClusterServiceUnitTest {

    private MockLogAppender appender;

    @Before
    public void appendLogger() throws Exception {
        appender = new MockLogAppender();
        appender.start();
        Loggers.addAppender(Loggers.getLogger(Publication.class), appender);

    }

    @After
    public void removeLogger() {
        Loggers.removeAppender(Loggers.getLogger(Publication.class), appender);
        appender.stop();
    }

    @Test
    public void test_resolve_relation_names_for_all_tables_ignores_table_with_soft_delete_disabled() throws Exception {
        var user = new User("dummy", Set.of(), Set.of(), null) {
            @Override
            public boolean hasPrivilege(Privilege.Type type, Privilege.Clazz clazz, String ident, String defaultSchema) {
                return true; // This test case doesn't check privileges.
            }
        };
        // Soft-deletes are mandatory from 5.0, so let's use 4.8 to create a table with soft-deletes disabled
        clusterService = createClusterService(additionalClusterSettings().stream().filter(Setting::hasNodeScope).toList(),
                                                  Metadata.EMPTY_METADATA,
                                                  Version.V_4_8_0);
        SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE doc.t1 (id int)")
            .addTable("CREATE TABLE doc.t2 (id int) with (\"soft_deletes.enabled\" = false)")
            .build();
        var publication = new Publication("some_user", true, List.of());

        var expectedLogMessage = "Table 'doc.t2' won't be replicated as the required table setting " +
                                 "'soft_deletes.enabled' is set to: false";
        appender.addExpectation(new MockLogAppender.SeenEventExpectation(
            expectedLogMessage,
            Loggers.getLogger(Publication.class).getName(),
            Level.WARN,
            expectedLogMessage
        ));

        Map<RelationName, RelationMetadata> resolvedRelations = publication.resolveCurrentRelations(clusterService.state(), user, user, "dummy");
        assertThat(resolvedRelations.keySet(), contains(new RelationName("doc", "t1")));
        appender.assertAllExpectationsMatched();
    }

    @Test
    public void test_resolve_relation_names_for_all_tables_ignores_table_when_pub_owner_doesnt_have_read_write_define_permissions() throws Exception {
        var publicationOwner = new User("publisher", Set.of(), Set.of(), null) {
            @Override
            public boolean hasPrivilege(Privilege.Type type, Privilege.Clazz clazz, String ident, String defaultSchema) {
                return ident.equals("doc.t1");
            }
        };
        var subscriber = new User("subscriber", Set.of(), Set.of(), null) {
            @Override
            public boolean hasPrivilege(Privilege.Type type, Privilege.Clazz clazz, String ident, String defaultSchema) {
                return true;
            }
        };

        UserLookup userLookup = mock(UserLookup.class);
        when(userLookup.findUser("publisher")).thenReturn(publicationOwner);
        when(userLookup.findUser("subscriber")).thenReturn(subscriber);

        SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE doc.t1 (id int)")
            .addTable("CREATE TABLE doc.t3 (id int)")
            .build();
        var publication = new Publication("publisher", true, List.of());

        var resolvedRelations = publication.resolveCurrentRelations(
            clusterService.state(),
            publicationOwner,
            subscriber,
            "dummy"
        );

        assertThat(resolvedRelations.keySet(), contains(new RelationName("doc", "t1")));
    }

    @Test
    public void test_resolve_relation_names_for_all_tables_ignores_table_when_subscriber_doesnt_have_read_permissions() throws Exception {
        var publicationOwner = new User("publisher", Set.of(), Set.of(), null) {
            @Override
            public boolean hasPrivilege(Privilege.Type type, Privilege.Clazz clazz, String ident, String defaultSchema) {
                return true;
            }
        };

        var subscriber = new User("subscriber", Set.of(), Set.of(), null) {
            @Override
            public boolean hasPrivilege(Privilege.Type type, Privilege.Clazz clazz, String ident, String defaultSchema) {
                return ident.equals("doc.t1");
            }
        };

        UserLookup userLookup = mock(UserLookup.class);
        when(userLookup.findUser("publisher")).thenReturn(publicationOwner);
        when(userLookup.findUser("subscriber")).thenReturn(subscriber);

        SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE doc.t1 (id int)")
            .addTable("CREATE TABLE doc.t3 (id int)")
            .build();
        var publication = new Publication("publisher", true, List.of());

        var resolvedRelations = publication.resolveCurrentRelations(clusterService.state(), publicationOwner, subscriber, "dummy");
        assertThat(resolvedRelations.keySet(), contains(new RelationName("doc", "t1")));
    }

    @Test
    public void test_resolve_relation_names_for_fixed_tables_ignores_table_when_subscriber_doesnt_have_read_permissions() throws Exception {
        var publicationOwner = new User("publisher", Set.of(), Set.of(), null) {
            @Override
            public boolean hasPrivilege(Privilege.Type type, Privilege.Clazz clazz, String ident, String defaultSchema) {
                return true;
            }
        };

        var subscriber = new User("subscriber", Set.of(), Set.of(), null) {
            @Override
            public boolean hasPrivilege(Privilege.Type type, Privilege.Clazz clazz, String ident, String defaultSchema) {
                return ident.equals("doc.t1");
            }
        };

        UserLookup userLookup = mock(UserLookup.class);
        when(userLookup.findUser("publisher")).thenReturn(publicationOwner);
        when(userLookup.findUser("subscriber")).thenReturn(subscriber);

        SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE doc.t1 (id int)")
            .addTable("CREATE TABLE doc.t2 (id int)")
            .build();
        var publication = new Publication("publisher", false,
            List.of(
                RelationName.of(QualifiedName.of("t1"), Schemas.DOC_SCHEMA_NAME),
                RelationName.of(QualifiedName.of("t2"), Schemas.DOC_SCHEMA_NAME)
            )
        );

        var resolvedRelations = publication.resolveCurrentRelations(
            clusterService.state(),
            publicationOwner,
            subscriber,
            "dummy"
        );
        assertThat(resolvedRelations.keySet(), contains(new RelationName("doc", "t1")));
    }
}
