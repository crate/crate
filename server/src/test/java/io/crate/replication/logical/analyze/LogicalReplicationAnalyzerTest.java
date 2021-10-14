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

import io.crate.exceptions.RelationUnknown;
import io.crate.metadata.RelationName;
import io.crate.replication.logical.exceptions.PublicationAlreadyExistsException;
import io.crate.replication.logical.exceptions.PublicationUnknownException;
import io.crate.replication.logical.exceptions.SubscriptionAlreadyExistsException;
import io.crate.replication.logical.exceptions.SubscriptionUnknownException;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class LogicalReplicationAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_create_publication_with_unknown_table_raise_error() {
        var e = SQLExecutor.builder(clusterService).build();
        expectThrows(
            RelationUnknown.class,
            () -> e.analyze("CREATE PUBLICATION pub1 FOR TABLE non_existing")
        );
    }

    @Test
    public void test_create_publication_which_already_exists_raises_error() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table doc.t1 (x int)")
            .addPublication("pub1", false, new RelationName("doc", "t1"))
            .build();

        expectThrows(
            PublicationAlreadyExistsException.class,
            () -> e.analyze("CREATE PUBLICATION pub1 FOR TABLE doc.t1")
        );
    }

    /**
     * The tables a subscription will follow on publications with `FOR ALL TABLES`
     * must be resolved on-demand instead while creating the publication.
     * This test ensures that no tables are resolved in that case.
     */
    @Test
    public void test_create_publication_for_all_tables_results_in_empty_table_list() {
        var e = SQLExecutor.builder(clusterService).build();
        AnalyzedCreatePublication stmt = e.analyze("CREATE PUBLICATION pub1 FOR ALL TABLES");
        assertThat(stmt.tables(), Matchers.empty());
        assertThat(stmt.isForAllTables(), is(true));
    }

    @Test
    public void test_create_publication_without_any_table_specification() {
        var e = SQLExecutor.builder(clusterService).build();
        AnalyzedCreatePublication stmt = e.analyze("CREATE PUBLICATION pub1");
        assertThat(stmt.tables(), Matchers.empty());
        assertThat(stmt.isForAllTables(), is(false));
    }

    @Test
    public void test_drop_unknown_publication_raises_error() {
        var e = SQLExecutor.builder(clusterService).build();
        expectThrows(
            PublicationUnknownException.class,
            () -> e.analyze("DROP PUBLICATION pub1")
        );
    }

    @Test
    public void test_drop_publication_if_exists_with_unknown_publication_does_not_raise_error() {
        var e = SQLExecutor.builder(clusterService).build();
        AnalyzedDropPublication stmt = e.analyze("DROP PUBLICATION IF EXISTS pub1");
        assertThat(stmt.ifExists(), is(true));
        assertThat(stmt.name(), is("pub1"));
    }

    @Test
    public void test_alter_unknown_publication_raises_error() {
        var e = SQLExecutor.builder(clusterService).build();
        expectThrows(
            PublicationUnknownException.class,
            () -> e.analyze("ALTER PUBLICATION pub1 SET TABLE t1")
        );
    }

    @Test
    public void test_alter_publication_with_unknown_table_raise_error() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table doc.t1 (x int)")
            .addPublication("pub1", false, new RelationName("doc", "t1"))
            .build();
        expectThrows(
            RelationUnknown.class,
            () -> e.analyze("ALTER PUBLICATION pub1 ADD TABLE non_existing")
        );
    }

    @Test
    public void test_create_subscription_which_already_exists_raises_error() {
        var e = SQLExecutor.builder(clusterService)
            .addSubscription("sub1", "pub1")
            .build();
        expectThrows(
            SubscriptionAlreadyExistsException.class,
            () -> e.analyze("CREATE SUBSCRIPTION sub1 CONNECTION 'crate://localhost' PUBLICATION pub1")
        );
    }

    @Test
    public void test_drop_unknown_subscription_raises_error() {
        var e = SQLExecutor.builder(clusterService).build();
        expectThrows(
            SubscriptionUnknownException.class,
            () -> e.analyze("DROP SUBSCRIPTION sub1")
        );
    }

    @Test
    public void test_drop_unknown_subscription_using_if_exists_does_not_raise_error() {
        var e = SQLExecutor.builder(clusterService).build();
        AnalyzedDropSubscription stmt = e.analyze("DROP SUBSCRIPTION IF EXISTS sub1");
        assertThat(stmt.ifExists(), is(true));
        assertThat(stmt.name(), is("sub1"));
    }

    @Test
    public void test_alter_unknown_subscription_raises_error() {
        var e = SQLExecutor.builder(clusterService).build();
        expectThrows(
            SubscriptionUnknownException.class,
            () -> e.analyze("ALTER SUBSCRIPTION sub1 DISABLE")
        );
    }
}
