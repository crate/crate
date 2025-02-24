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

package io.crate.replication.logical.analyze;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import io.crate.analyze.ParamTypeHints;
import io.crate.exceptions.InvalidArgumentException;
import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.UnauthorizedException;
import io.crate.metadata.RelationName;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.replication.logical.exceptions.PublicationAlreadyExistsException;
import io.crate.replication.logical.exceptions.PublicationUnknownException;
import io.crate.replication.logical.exceptions.SubscriptionAlreadyExistsException;
import io.crate.replication.logical.exceptions.SubscriptionUnknownException;
import io.crate.role.metadata.RolesHelper;
import io.crate.sql.parser.SqlParser;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class LogicalReplicationAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_create_publication_with_unknown_table_raise_error() {
        var e = SQLExecutor.of(clusterService);
        assertThatThrownBy(() -> e.analyze("CREATE PUBLICATION pub1 FOR TABLE non_existing"))
            .isExactlyInstanceOf(RelationUnknown.class);
    }

    @Test
    public void test_create_publication_which_already_exists_raises_error() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table doc.t1 (x int)")
            .addPublication("pub1", false, new RelationName("doc", "t1"));

        assertThatThrownBy(() -> e.analyze("CREATE PUBLICATION pub1 FOR TABLE doc.t1"))
            .isExactlyInstanceOf(PublicationAlreadyExistsException.class);
    }

    /**
     * The tables a subscription will follow on publications with `FOR ALL TABLES`
     * must be resolved on-demand instead while creating the publication.
     * This test ensures that no tables are resolved in that case.
     */
    @Test
    public void test_create_publication_for_all_tables_results_in_empty_table_list() {
        var e = SQLExecutor.of(clusterService);
        AnalyzedCreatePublication stmt = e.analyze("CREATE PUBLICATION pub1 FOR ALL TABLES");
        assertThat(stmt.tables()).isEmpty();
        assertThat(stmt.isForAllTables()).isTrue();
    }

    @Test
    public void test_create_publication_without_any_table_specification() {
        var e = SQLExecutor.of(clusterService);
        AnalyzedCreatePublication stmt = e.analyze("CREATE PUBLICATION pub1");
        assertThat(stmt.tables()).isEmpty();
        assertThat(stmt.isForAllTables()).isFalse();
    }

    @Test
    public void test_drop_unknown_publication_raises_error() {
        var e = SQLExecutor.of(clusterService);
        assertThatThrownBy(() -> e.analyze("DROP PUBLICATION pub1"))
            .isExactlyInstanceOf(PublicationUnknownException.class);
    }

    @Test
    public void test_drop_publication_if_exists_with_unknown_publication_does_not_raise_error() {
        var e = SQLExecutor.of(clusterService);
        AnalyzedDropPublication stmt = e.analyze("DROP PUBLICATION IF EXISTS pub1");
        assertThat(stmt.ifExists()).isTrue();
        assertThat(stmt.name()).isEqualTo("pub1");
    }

    @Test
    public void test_drop_publication_as_non_superuser_and_non_owner_raises_error() {
        var e = SQLExecutor.of(clusterService)
            .setUser(RolesHelper.userOf("owner"))
            .addPublication("pub1", true);
        Assertions.assertThatThrownBy(() -> e.analyzer.analyze(
                        SqlParser.createStatement("DROP PUBLICATION pub1"),
                        new CoordinatorSessionSettings(RolesHelper.userOf("other_user")),
                        ParamTypeHints.EMPTY,
                        e.cursors
            ))
            .isExactlyInstanceOf(UnauthorizedException.class)
            .hasMessageContaining("A publication can only be dropped by the owner or a superuser");
    }

    @Test
    public void test_alter_unknown_publication_raises_error() {
        var e = SQLExecutor.of(clusterService);
        assertThatThrownBy(() -> e.analyze("ALTER PUBLICATION pub1 SET TABLE t1"))
            .isExactlyInstanceOf(PublicationUnknownException.class);
    }

    @Test
    public void test_alter_publication_with_unknown_table_raise_error() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table doc.t1 (x int)")
            .addPublication("pub1", false, new RelationName("doc", "t1"));
        assertThatThrownBy(() -> e.analyze("ALTER PUBLICATION pub1 ADD TABLE non_existing"))
            .isExactlyInstanceOf(RelationUnknown.class);
    }

    @Test
    public void test_alter_publication_for_all_tables_raise_error() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table doc.t1 (x int)")
            .addPublication("pub1", true);
        Assertions.assertThatThrownBy(() -> e.analyze("ALTER PUBLICATION pub1 ADD TABLE doc.t1"))
            .isExactlyInstanceOf(InvalidArgumentException.class)
            .hasMessageContaining(
                    "Publication 'pub1' is defined as FOR ALL TABLES, adding or dropping tables is not supported");
    }

    @Test
    public void test_alter_publication_as_non_superuser_and_non_owner_raises_error() {
        var e = SQLExecutor.of(clusterService)
            .setUser(RolesHelper.userOf("owner"))
            .addPublication("pub1", false, new RelationName("doc", "t1"));
        Assertions.assertThatThrownBy(() -> e.analyzer.analyze(
                        SqlParser.createStatement("ALTER PUBLICATION pub1 ADD TABLE doc.t2"),
                        new CoordinatorSessionSettings(RolesHelper.userOf("other_user")),
                        ParamTypeHints.EMPTY,
                        e.cursors
            ))
            .isExactlyInstanceOf(UnauthorizedException.class)
            .hasMessageContaining("A publication can only be altered by the owner or a superuser");
    }

    @Test
    public void test_create_subscription_which_already_exists_raises_error() {
        var e = SQLExecutor.of(clusterService)
            .addSubscription("sub1", "pub1");
        assertThatThrownBy(() -> e.analyze("CREATE SUBSCRIPTION sub1 CONNECTION 'crate://localhost' PUBLICATION pub1"))
            .isExactlyInstanceOf(SubscriptionAlreadyExistsException.class);
    }

    @Test
    public void test_drop_unknown_subscription_raises_error() {
        var e = SQLExecutor.of(clusterService);
        assertThatThrownBy(() -> e.analyze("DROP SUBSCRIPTION sub1"))
            .isExactlyInstanceOf(SubscriptionUnknownException.class);
    }

    @Test
    public void test_drop_unknown_subscription_using_if_exists_does_not_raise_error() {
        var e = SQLExecutor.of(clusterService);
        AnalyzedDropSubscription stmt = e.analyze("DROP SUBSCRIPTION IF EXISTS sub1");
        assertThat(stmt.ifExists()).isTrue();
        assertThat(stmt.name()).isEqualTo("sub1");
    }

    @Test
    public void test_drop_subscription_as_non_superuser_and_non_owner_raises_error() {
        var e = SQLExecutor.of(clusterService)
            .setUser(RolesHelper.userOf("owner"))
            .addSubscription("sub1", "pub1");
        Assertions.assertThatThrownBy(() -> e.analyzer.analyze(
                        SqlParser.createStatement("DROP SUBSCRIPTION sub1"),
                        new CoordinatorSessionSettings(RolesHelper.userOf("other_user")),
                        ParamTypeHints.EMPTY,
                        e.cursors
            ))
            .isExactlyInstanceOf(UnauthorizedException.class)
            .hasMessageContaining("A subscription can only be dropped by the owner or a superuser");
    }

    @Test
    public void test_alter_subscription_as_non_superuser_and_non_owner_raises_error() {
        var e = SQLExecutor.of(clusterService)
            .setUser(RolesHelper.userOf("owner"))
            .addSubscription("sub1", "pub1");
        Assertions.assertThatThrownBy(() -> e.analyzer.analyze(
                        SqlParser.createStatement("ALTER SUBSCRIPTION sub1 DISABLE"),
                        new CoordinatorSessionSettings(RolesHelper.userOf("other_user")),
                        ParamTypeHints.EMPTY,
                        e.cursors
            ))
            .isExactlyInstanceOf(UnauthorizedException.class)
            .hasMessageContaining("A subscription can only be altered by the owner or a superuser");
    }

    @Test
    public void test_alter_unknown_subscription_raises_error() {
        var e = SQLExecutor.of(clusterService);
        assertThatThrownBy(() -> e.analyze("ALTER SUBSCRIPTION sub1 DISABLE"))
            .isExactlyInstanceOf(SubscriptionUnknownException.class);
    }

    @Test
    public void test_cannot_create_publication_for_system_table() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService);
        assertThatThrownBy(() -> e.plan("create publication pub1 for table sys.summits"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessage("The relation \"sys.summits\" doesn't support or allow CREATE PUBLICATION operations");
    }
}
