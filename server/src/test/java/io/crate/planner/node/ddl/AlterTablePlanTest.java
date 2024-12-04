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

package io.crate.planner.node.ddl;

import static io.crate.replication.logical.LogicalReplicationSettings.REPLICATION_SUBSCRIPTION_NAME;
import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.BoundAlterTable;
import io.crate.data.Row;
import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.planner.operators.SubQueryResults;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class AlterTablePlanTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable(
                "create table doc.test(i int)",
                Settings.builder()
                    .put(REPLICATION_SUBSCRIPTION_NAME.getKey(), "sub1")
                    .build()
            );
    }

    /**
     * https://github.com/crate/crate/issues/12478
     */
    @Test
    public void test_alter_allowed_settings_on_a_replicated_table() throws IOException {

        assertThat(analyze("Alter table doc.test set(number_of_replicas = 1)")).isNotNull();

        assertThat(analyze("Alter table doc.test set(refresh_interval = 523)")).isNotNull();

    }

    @Test
    public void test_alter_forbidden_settings_on_a_replicated_table() throws IOException {
        assertThatThrownBy(() -> analyze("Alter table doc.test set(number_of_shards = 1)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid property \"number_of_shards\" passed to [ALTER | CREATE] TABLE statement");
    }

    @Test
    public void testAlterBlobTableWithInvalidProperty() throws Throwable {
        e.addBlobTable("create blob table blobs");
        assertThatThrownBy(() -> analyze("alter blob table blobs set (foobar='2')"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid property \"foobar\" passed to [ALTER | CREATE] TABLE statement");
    }

    @Test
    public void testAlterBlobTableWithReplicas() throws Throwable {
        e.addBlobTable("create blob table blobs");
        BoundAlterTable alterTable = analyze("alter blob table blobs set (number_of_replicas=2)");
        assertThat(alterTable.table().ident().name()).isEqualTo("blobs");
        assertThat(alterTable.settings().getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
            .isEqualTo(2);
    }

    @Test
    public void test_alter_setting_block_read_only() throws Throwable {
        e.addBlobTable("create blob table blobs");
        BoundAlterTable alterTable = analyze("alter blob table blobs set (\"blocks.read_only_allow_delete\"=true)");
        assertThat(alterTable.table().ident().name()).isEqualTo("blobs");
        assertThat(alterTable.settings().getAsBoolean(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE, false)).isTrue();
    }

    @Test
    public void testAlterBlobTableWithPath() throws Throwable {
        e.addBlobTable("create blob table blobs");
        assertThatThrownBy(() -> analyze("alter blob table blobs set (blobs_path=1)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid property \"blobs_path\" passed to [ALTER | CREATE] TABLE statement");
    }

    @Test
    public void testAlterBlobTableRenameTable() throws Throwable {
        e.addBlobTable("create blob table blobs");
        assertThatThrownBy(() -> e.analyze("alter blob table blobs rename to blobbier"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessage("The relation \"blob.blobs\" doesn't support or allow ALTER RENAME operations");
    }

    @Test
    public void testAlterBlobTableRenameTableWithExplicitSchema() throws Throwable {
        e.addBlobTable("create blob table blobs");
        assertThatThrownBy(() -> e.analyze("alter blob table schema.blobs rename to blobbier"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("The Schema \"schema\" isn't valid in a [CREATE | ALTER] BLOB TABLE clause");
    }

    @Test
    public void testAlterBlobTableOpenClose() throws Throwable {
        e.addBlobTable("create blob table blobs");
        assertThatThrownBy(() -> e.analyze("alter blob table blobs close"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessage("The relation \"blob.blobs\" doesn't support or allow ALTER CLOSE operations");
    }

    @Test
    public void testAlterBlobTableOpenCloseWithExplicitSchema() {
        assertThatThrownBy(() -> e.analyze("alter blob table schema.blob close"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("The Schema \"schema\" isn't valid in a [CREATE | ALTER] BLOB TABLE clause");
    }


    private BoundAlterTable analyze(String stmt) {
        AlterTablePlan plan = e.plan(stmt);
        return AlterTablePlan.bind(
            plan.alterTable,
            CoordinatorTxnCtx.systemTransactionContext(),
            createNodeContext(),
            Row.EMPTY,
            SubQueryResults.EMPTY,
            e.getPlannerContext().clusterState().metadata()
        );
    }
}
