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

package io.crate.analyze;

import static io.crate.planner.node.ddl.AlterTablePlan.getTableParameter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.function.Function;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import io.crate.blob.v2.BlobIndicesService;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.exceptions.InvalidRelationName;
import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.exceptions.RelationAlreadyExists;
import io.crate.exceptions.RelationUnknown;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.ddl.CreateBlobTablePlan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.AlterTable;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class BlobTableAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private static Function<? super Symbol, Object> EVAL = x -> SymbolEvaluator.evaluate(
        null,
        null,
        x,
        Row.EMPTY,
        SubQueryResults.EMPTY
    );

    private SQLExecutor e;
    private PlannerContext plannerContext;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.of(clusterService).addBlobTable("create blob table blobs");
        plannerContext = e.getPlannerContext(clusterService.state());
    }

    private Settings buildSettings(AnalyzedCreateBlobTable blobTable, Object... arguments) {
        return CreateBlobTablePlan.buildSettings(
            blobTable.createBlobTable(),
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            new RowN(arguments),
            SubQueryResults.EMPTY,
            new NumberOfShards(clusterService));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithInvalidProperty() {
        AnalyzedCreateBlobTable blobTable = e.analyze("create blob table screenshots with (foobar=1)");
        buildSettings(blobTable);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithMultipleArgsToProperty() {
        AnalyzedCreateBlobTable blobTable = e.analyze("create blob table screenshots with (number_of_replicas=[1, 2])");
        buildSettings(blobTable);
    }

    @Test
    public void testCreateBlobTableAutoExpand() {
        AnalyzedCreateBlobTable analysis = e.analyze(
            "create blob table screenshots clustered into 10 shards with (number_of_replicas='0-all')");
        Settings settings = buildSettings(analysis);

        assertThat(analysis.relationName().name()).isEqualTo("screenshots");
        assertThat(analysis.relationName().schema()).isEqualTo(BlobSchemaInfo.NAME);
        assertThat(settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 0)).isEqualTo(10);
        assertThat(settings.get(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS)).isEqualTo("0-all");
    }

    @Test
    public void testCreateBlobTableDefaultNumberOfShards() {
        AnalyzedCreateBlobTable analysis = e.analyze("create blob table screenshots");
        Settings settings = buildSettings(analysis);

        assertThat(analysis.relationName().name()).isEqualTo("screenshots");
        assertThat(analysis.relationName().schema()).isEqualTo(BlobSchemaInfo.NAME);
        assertThat(settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 0)).isEqualTo(4);
    }

    @Test
    public void testCreateBlobTableRaisesErrorIfAlreadyExists() {
        assertThatThrownBy(() -> e.analyze("create blob table blobs"))
            .isExactlyInstanceOf(RelationAlreadyExists.class);
    }

    @Test
    public void testCreateBlobTable() {
        AnalyzedCreateBlobTable analysis = e.analyze(
            "create blob table screenshots clustered into 10 shards with (number_of_replicas='0-all')");
        Settings settings = buildSettings(analysis);

        assertThat(analysis.relationName().name()).isEqualTo("screenshots");
        assertThat(settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 0)).isEqualTo(10);
        assertThat(settings.get(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS)).isEqualTo("0-all");
    }

    @Test
    public void testCreateBlobTableWithPath() {
        AnalyzedCreateBlobTable analysis = e.analyze(
            "create blob table screenshots with (blobs_path='/tmp/crate_blob_data')");
        Settings settings = buildSettings(analysis);

        assertThat(analysis.relationName().name()).isEqualTo("screenshots");
        assertThat(settings.get(BlobIndicesService.SETTING_INDEX_BLOBS_PATH.getKey())).isEqualTo(
            "/tmp/crate_blob_data");
    }

    @Test
    public void testCreateBlobTableWithPathParameter() {
        AnalyzedCreateBlobTable analysis = e.analyze(
            "create blob table screenshots with (blobs_path=?)");
        Settings settings = buildSettings(analysis, "/tmp/crate_blob_data");

        assertThat(analysis.relationName().name()).isEqualTo("screenshots");
        assertThat(settings.get(BlobIndicesService.SETTING_INDEX_BLOBS_PATH.getKey())).isEqualTo(
            "/tmp/crate_blob_data");
    }

    @Test
    public void testCreateBlobTableWithPathInvalidType() {
        assertThatThrownBy(() -> buildSettings(e.analyze("create blob table screenshots with (blobs_path=1)")))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid value for argument 'blobs_path'");
    }

    @Test
    public void testCreateBlobTableWithPathInvalidParameter() {
        assertThatThrownBy(() -> buildSettings(e.analyze("create blob table screenshots with (blobs_path=?)"), 1))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid value for argument 'blobs_path'");
    }

    @Test(expected = InvalidRelationName.class)
    public void testCreateBlobTableIllegalTableName() {
        e.analyze("create blob table \"blob.s\"");
    }

    @Test
    public void testDropBlobTable() {
        AnalyzedDropTable<BlobTableInfo> analysis = e.analyze("drop blob table blobs");
        assertThat(analysis.table().ident().name()).isEqualTo("blobs");
        assertThat(analysis.table().ident().schema()).isEqualTo(BlobSchemaInfo.NAME);
    }

    @Test
    public void testDropBlobTableWithInvalidSchema() {
        assertThatThrownBy(() -> e.analyze("drop blob table doc.users"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("No blob tables in schema `doc`");
    }

    @Test
    public void testDropBlobTableWithValidSchema() {
        AnalyzedDropTable<BlobTableInfo> analysis = e.analyze("drop blob table \"blob\".blobs");
        assertThat(analysis.table().ident().name()).isEqualTo("blobs");
    }

    @Test
    public void testDropBlobTableThatDoesNotExist() {
        assertThatThrownBy(() -> e.analyze("drop blob table unknown"))
            .isExactlyInstanceOf(RelationUnknown.class);

    }

    @Test
    public void testDropBlobTableIfExists() {
        AnalyzedDropTable<BlobTableInfo> analysis = e.analyze("drop blob table if exists blobs");
        assertThat(analysis.dropIfExists()).isTrue();
        assertThat(analysis.table().ident().fqn()).isEqualTo("blob.blobs");
    }

    @Test
    public void testDropNonExistentBlobTableIfExists() {
        AnalyzedDropTable<BlobTableInfo> analysis = e.analyze("drop blob table if exists unknown");
        assertThat(analysis.dropIfExists()).isTrue();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAlterBlobTableWithInvalidProperty() {
        AnalyzedAlterBlobTable analysis = e.analyze("alter blob table blobs set (foobar='2')");
        AlterTable<Object> alterTable = analysis.alterTable().map(EVAL);
        getTableParameter(alterTable, TableParameters.ALTER_BLOB_TABLE_PARAMETERS);
    }

    @Test
    public void testAlterBlobTableWithReplicas() {
        AnalyzedAlterBlobTable analysis = e.analyze("alter blob table blobs set (number_of_replicas=2)");
        assertThat(analysis.tableInfo().ident().name()).isEqualTo("blobs");
        AlterTable<Object> alterTable = analysis.alterTable().map(EVAL);
        TableParameter parameter = getTableParameter(alterTable, TableParameters.ALTER_BLOB_TABLE_PARAMETERS);
        assertThat(parameter.settings().getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)).isEqualTo(2);
    }

    @Test
    public void test_alter_setting_block_read_only() {
        AnalyzedAlterBlobTable analysis = e.analyze("alter blob table blobs set (\"blocks.read_only_allow_delete\"=true)");
        assertThat(analysis.tableInfo().ident().name()).isEqualTo("blobs");
        AlterTable<Object> alterTable = analysis.alterTable().map(EVAL);
        TableParameter parameter = getTableParameter(alterTable, TableParameters.ALTER_BLOB_TABLE_PARAMETERS);
        assertThat(parameter.settings().getAsBoolean(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE, false)).isTrue();
    }

    @Test
    public void testAlterBlobTableWithPath() {
        AnalyzedAlterBlobTable analysis = e.analyze("alter blob table blobs set (blobs_path=1)");
        AlterTable<Object> alterTable = analysis.alterTable().map(EVAL);
        assertThatThrownBy(() -> getTableParameter(alterTable, TableParameters.ALTER_BLOB_TABLE_PARAMETERS))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid property \"blobs_path\" passed to [ALTER | CREATE] TABLE statement");
    }

    @Test
    public void testCreateBlobTableWithParams() {
        AnalyzedCreateBlobTable analysis = e.analyze(
            "create blob table screenshots clustered into ? shards with (number_of_replicas= ?)");
        Settings settings = buildSettings(analysis, 2, "0-all");

        assertThat(analysis.relationName().name()).isEqualTo("screenshots");
        assertThat(analysis.relationName().schema()).isEqualTo(BlobSchemaInfo.NAME);
        assertThat(settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 0)).isEqualTo(2);
        assertThat(settings.get(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS)).isEqualTo("0-all");
    }

    @Test
    public void testCreateBlobTableWithInvalidShardsParam() {
        assertThatThrownBy(() -> buildSettings(e.analyze("create blob table screenshots clustered into ? shards"), "foo"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("invalid number 'foo'");
    }

    @Test
    public void testAlterBlobTableRenameTable() {
        assertThatThrownBy(() -> e.analyze("alter blob table blobs rename to blobbier"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessage("The relation \"blob.blobs\" doesn't support or allow ALTER RENAME operations.");
    }

    @Test
    public void testAlterBlobTableRenameTableWithExplicitSchema() {
        assertThatThrownBy(() -> e.analyze("alter blob table schema.blobs rename to blobbier"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("The Schema \"schema\" isn't valid in a [CREATE | ALTER] BLOB TABLE clause");
    }

    @Test
    public void testAlterBlobTableOpenClose() {
        assertThatThrownBy(() -> e.analyze("alter blob table blobs close"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessage("The relation \"blob.blobs\" doesn't support or allow ALTER CLOSE operations.");
    }

    @Test
    public void testAlterBlobTableOpenCloseWithExplicitSchema() {
        assertThatThrownBy(() -> e.analyze("alter blob table schema.blob close"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("The Schema \"schema\" isn't valid in a [CREATE | ALTER] BLOB TABLE clause");
    }
}
