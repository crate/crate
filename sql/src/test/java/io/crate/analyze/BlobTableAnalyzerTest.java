/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import io.crate.blob.v2.BlobIndicesService;
import io.crate.data.Row;
import io.crate.exceptions.InvalidRelationName;
import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.exceptions.RelationAlreadyExists;
import io.crate.exceptions.RelationUnknown;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.AlterTable;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.function.Function;

import static io.crate.planner.node.ddl.AlterTablePlan.getTableParameter;
import static org.hamcrest.Matchers.is;

public class BlobTableAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private static Function<? super Symbol, Object> EVAL = x -> SymbolEvaluator.evaluate(
        null,
        null,
        x,
        Row.EMPTY,
        SubQueryResults.EMPTY
    );


    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.builder(clusterService).addBlobTable("create blob table blobs").build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithInvalidProperty() {
        e.analyze("create blob table screenshots with (foobar=1)");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithMultipleArgsToProperty() {
        e.analyze("create blob table screenshots with (number_of_replicas=[1, 2])");
    }

    @Test
    public void testCreateBlobTableAutoExpand() {
        CreateBlobTableAnalyzedStatement analysis = e.analyze(
            "create blob table screenshots clustered into 10 shards with (number_of_replicas='0-all')");

        assertThat(analysis.tableIdent().name(), is("screenshots"));
        assertThat(analysis.tableIdent().schema(), is(BlobSchemaInfo.NAME));
        assertThat(analysis.tableParameter().settings().getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 0), is(10));
        assertThat(analysis.tableParameter().settings().get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("0-all"));
    }

    @Test
    public void testCreateBlobTableDefaultNumberOfShards() {
        CreateBlobTableAnalyzedStatement analysis = e.analyze("create blob table screenshots");
        assertThat(analysis.tableIdent().name(), is("screenshots"));
        assertThat(analysis.tableIdent().schema(), is(BlobSchemaInfo.NAME));
        assertThat(analysis.tableParameter().settings().getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 0), is(4));
    }

    @Test
    public void testCreateBlobTableRaisesErrorIfAlreadyExists() {
        expectedException.expect(RelationAlreadyExists.class);
        e.analyze("create blob table blobs");
    }

    @Test
    public void testCreateBlobTable() {
        CreateBlobTableAnalyzedStatement analysis = e.analyze(
            "create blob table screenshots clustered into 10 shards with (number_of_replicas='0-all')");

        assertThat(analysis.tableIdent().name(), is("screenshots"));
        assertThat(analysis.tableParameter().settings().getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 0), is(10));
        assertThat(analysis.tableParameter().settings().get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("0-all"));
    }

    @Test
    public void testCreateBlobTableWithPath() {
        CreateBlobTableAnalyzedStatement analysis = e.analyze(
            "create blob table screenshots with (blobs_path='/tmp/crate_blob_data')");

        assertThat(analysis.tableIdent().name(), is("screenshots"));
        assertThat(analysis.tableParameter().settings().get(BlobIndicesService.SETTING_INDEX_BLOBS_PATH.getKey()),
            is("/tmp/crate_blob_data"));
    }

    @Test
    public void testCreateBlobTableWithPathParameter() {
        CreateBlobTableAnalyzedStatement analysis = e.analyze(
            "create blob table screenshots with (blobs_path=?)", new Object[]{"/tmp/crate_blob_data"});

        assertThat(analysis.tableIdent().name(), is("screenshots"));
        assertThat(analysis.tableParameter().settings().get(BlobIndicesService.SETTING_INDEX_BLOBS_PATH.getKey()),
            is("/tmp/crate_blob_data"));
    }

    @Test
    public void testCreateBlobTableWithPathInvalidType() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid value for argument 'blobs_path'");
        e.analyze("create blob table screenshots with (blobs_path=1)");
    }

    @Test
    public void testCreateBlobTableWithPathInvalidParameter() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid value for argument 'blobs_path'");
        e.analyze("create blob table screenshots with (blobs_path=?)", new Object[]{1});
    }

    @Test(expected = InvalidRelationName.class)
    public void testCreateBlobTableIllegalTableName() {
        e.analyze("create blob table \"blob.s\"");
    }

    @Test
    public void testDropBlobTable() {
        DropTableAnalyzedStatement<BlobTableInfo> analysis = e.analyze("drop blob table blobs");
        assertThat(analysis.table().ident().name(), is("blobs"));
        assertThat(analysis.table().ident().schema(), is(BlobSchemaInfo.NAME));
    }

    @Test
    public void testDropBlobTableWithInvalidSchema() {
        expectedException.expectMessage("No blob tables in schema `doc`");
        e.analyze("drop blob table doc.users");
    }

    @Test
    public void testDropBlobTableWithValidSchema() {
        DropTableAnalyzedStatement<BlobTableInfo> analysis = e.analyze("drop blob table \"blob\".blobs");
        assertThat(analysis.table().ident().name(), is("blobs"));
    }

    @Test(expected = RelationUnknown.class)
    public void testDropBlobTableThatDoesNotExist() {
        e.analyze("drop blob table unknown");
    }

    @Test
    public void testDropBlobTableIfExists() {
        DropTableAnalyzedStatement<BlobTableInfo> analysis = e.analyze("drop blob table if exists blobs");
        assertThat(analysis.dropIfExists(), is(true));
        assertThat(analysis.table().ident().fqn(), is("blob.blobs"));
    }

    @Test
    public void testDropNonExistentBlobTableIfExists() {
        DropTableAnalyzedStatement<BlobTableInfo> analysis = e.analyze("drop blob table if exists unknown");
        assertThat(analysis.dropIfExists(), is(true));
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
        assertThat(analysis.tableInfo().ident().name(), is("blobs"));
        AlterTable<Object> alterTable = analysis.alterTable().map(EVAL);
        TableParameter parameter = getTableParameter(alterTable, TableParameters.ALTER_BLOB_TABLE_PARAMETERS);
        assertThat(parameter.settings().getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0), is(2));
    }

    @Test
    public void testAlterBlobTableWithPath() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid property \"blobs_path\" passed to [ALTER | CREATE] TABLE statement");
        AnalyzedAlterBlobTable analysis = e.analyze("alter blob table blobs set (blobs_path=1)");
        AlterTable<Object> alterTable = analysis.alterTable().map(EVAL);
        getTableParameter(alterTable, TableParameters.ALTER_BLOB_TABLE_PARAMETERS);
    }

    @Test
    public void testCreateBlobTableWithParams() {
        CreateBlobTableAnalyzedStatement analysis = e.analyze(
            "create blob table screenshots clustered into ? shards with (number_of_replicas= ?)",
            new Object[]{2, "0-all"});

        assertThat(analysis.tableIdent().name(), is("screenshots"));
        assertThat(analysis.tableIdent().schema(), is(BlobSchemaInfo.NAME));
        assertThat(analysis.tableParameter().settings().getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 0), is(2));
        assertThat(analysis.tableParameter().settings().get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("0-all"));
    }

    @Test
    public void testCreateBlobTableWithInvalidShardsParam() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid number 'foo'");
        e.analyze("create blob table screenshots clustered into ? shards", new Object[]{"foo"});
    }

    @Test
    public void testAlterBlobTableRename() {
        expectedException.expect(OperationOnInaccessibleRelationException.class);
        expectedException.expectMessage("The relation \"blob.blobs\" doesn't support or allow ALTER RENAME operations.");
        e.analyze("alter blob table blobs rename to blobbier");
    }

    @Test
    public void testAlterBlobTableRenameWithExplicitSchema() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The Schema \"schema\" isn't valid in a [CREATE | ALTER] BLOB TABLE clause");
        e.analyze("alter blob table schema.blobs rename to blobbier");
    }

    @Test
    public void testAlterBlobTableOpenClose() {
        expectedException.expect(OperationOnInaccessibleRelationException.class);
        expectedException.expectMessage("The relation \"blob.blobs\" doesn't support or allow ALTER OPEN/CLOSE operations.");
        e.analyze("alter blob table blobs close");
    }

    @Test
    public void testAlterBlobTableOpenCloseWithExplicitSchema() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The Schema \"schema\" isn't valid in a [CREATE | ALTER] BLOB TABLE clause");
        e.analyze("alter blob table schema.blob close");
    }
}
