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

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static io.crate.analyze.TableDefinitions.TEST_PARTITIONED_TABLE_IDENT;
import static io.crate.analyze.TableDefinitions.USER_TABLE_IDENT;
import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.Asserts.isFunction;
import static io.crate.testing.Asserts.isLiteral;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import io.crate.data.RowN;
import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.exceptions.PartitionUnknownException;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.execution.dsl.projection.WriterProjection;
import io.crate.expression.scalar.ConcatFunction;
import io.crate.expression.scalar.CurrentDateFunction;
import io.crate.expression.scalar.cast.ImplicitCastFunction;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.planner.statement.CopyFromPlan;
import io.crate.planner.statement.CopyToPlan;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class CopyAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private PlannerContext plannerContext;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .addPartitionedTable(
                TableDefinitions.TEST_PARTITIONED_TABLE_DEFINITION,
                TableDefinitions.TEST_PARTITIONED_TABLE_PARTITIONS
            );
        plannerContext = e.getPlannerContext();
    }

    @SuppressWarnings("unchecked")
    private <S> S analyze(String stmt, Object... arguments) {
        AnalyzedStatement analyzedStatement = e.analyze(stmt);
        if (analyzedStatement instanceof AnalyzedCopyFrom) {
            return (S) CopyFromPlan.bind(
                (AnalyzedCopyFrom) analyzedStatement,
                plannerContext.transactionContext(),
                plannerContext.nodeContext(),
                new RowN(arguments),
                SubQueryResults.EMPTY
            );
        } else if (analyzedStatement instanceof AnalyzedCopyTo) {
            return (S) CopyToPlan.bind(
                (AnalyzedCopyTo) analyzedStatement,
                plannerContext.transactionContext(),
                plannerContext.nodeContext(),
                new RowN(arguments),
                SubQueryResults.EMPTY,
                plannerContext.clusterState().metadata()
            );
        }
        throw new UnsupportedOperationException("");
    }

    @Test
    public void testCopyFromExistingTable() throws Exception {
        BoundCopyFrom analysis = analyze(
            "COPY users FROM '/some/distant/file.ext'");
        assertThat(analysis.tableInfo().ident()).isEqualTo(USER_TABLE_IDENT);
        assertThat(analysis.uri()).isLiteral("/some/distant/file.ext");
    }

    @Test
    public void testCopyFromExistingPartitionedTable() {
        BoundCopyFrom analysis = analyze("COPY parted FROM '/some/distant/file.ext'");
        assertThat(analysis.tableInfo().ident()).isEqualTo(TEST_PARTITIONED_TABLE_IDENT);
        assertThat(analysis.uri()).isLiteral("/some/distant/file.ext");
    }

    @Test
    public void testCopyFromPartitionedTablePARTITIONKeywordTooManyArgs() throws Exception {
        assertThatThrownBy(() -> analyze("COPY parted PARTITION (a=1, b=2, c=3) FROM '/some/distant/file.ext'"))
            .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testCopyFromPartitionedTablePARTITIONKeywordValidArgs() throws Exception {
        BoundCopyFrom analysis = analyze(
            "COPY parted PARTITION (date=1395874800000) FROM '/some/distant/file.ext'");
        String parted = new PartitionName(
            new RelationName("doc", "parted"), Collections.singletonList("1395874800000")).ident();
        assertThat(analysis.partitionIdent()).isEqualTo(parted);
    }

    @Test
    public void testCopyFromNonExistingTable() throws Exception {
        assertThatThrownBy(() -> analyze("COPY unknown FROM '/some/distant/file.ext'"))
            .isExactlyInstanceOf(RelationUnknown.class);
    }

    @Test
    public void testCopyFromSystemTable() throws Exception {
        assertThatThrownBy(() -> analyze("COPY sys.shards FROM '/nope/nope/still.nope'"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessage(
                "The relation \"sys.shards\" doesn't support or allow INSERT " +
                "operations");
    }

    @Test
    public void testCopyFromWithColumnList() throws Exception {
        BoundCopyFrom analysis = analyze("COPY users (id, name) FROM  '/some/distant/file.ext'");
        List<String> outputs = analysis.targetColumns();
        assertThat(outputs).hasSize(2);
        assertThat(outputs.get(0)).isEqualTo("id");
        assertThat(outputs.get(1)).isEqualTo("name");
    }

    @Test
    public void testCopyFromUnknownSchema() throws Exception {
        assertThatThrownBy(() -> analyze("COPY suess.shards FROM '/nope/nope/still.nope'"))
            .isExactlyInstanceOf(SchemaUnknownException.class);
    }

    @Test
    public void testCopyFromParameter() throws Exception {
        String path = "/some/distant/file.ext";
        BoundCopyFrom analysis = analyze("COPY users FROM ?", new Object[]{path});
        assertThat(analysis.tableInfo().ident()).isEqualTo(USER_TABLE_IDENT);
        assertThat(analysis.uri()).isLiteral(path);
    }

    @Test
    public void convertCopyFrom_givenFormatIsSetToJsonInStatement_thenInputFormatIsSetToJson() {
        BoundCopyFrom analysis = analyze(
            "COPY users FROM '/some/distant/file.ext' WITH (format='json')");
        assertThat(analysis.inputFormat()).isEqualTo(FileUriCollectPhase.InputFormat.JSON);
    }

    @Test
    public void convertCopyFrom_givenFormatIsSetToCsvInStatement_thenInputFormatIsSetToCsv() {
        BoundCopyFrom analysis = analyze(
            "COPY users FROM '/some/distant/file.ext' WITH (format='csv')");
        assertThat(analysis.inputFormat()).isEqualTo(FileUriCollectPhase.InputFormat.CSV);
    }

    @Test
    public void test_copy_from_supports_empty_string_as_null_setting_option() {
        BoundCopyFrom analysis = analyze(
            "COPY users FROM '/some/distant/file.ext' WITH (format='csv', empty_string_as_null=true)");
        assertThat(analysis.settings()).isEqualTo(
            (Settings.builder()
                .put("empty_string_as_null", true)
                .put("format", "csv")
                .build()));
    }

    @Test
    public void test_copy_from_supports_header_setting_option() {
        BoundCopyFrom analysis = analyze(
            "COPY users FROM '/some/distant/file.ext' WITH (format='csv', header=false)");
        assertThat(analysis.settings()).isEqualTo(
            (Settings.builder()
                .put("format", "csv")
                .put("header", false)
                .build()));
    }

    @Test
    public void convertCopyFrom_givenFormatIsNotSetInStatement_thenInputFormatDefaultsToJson() {
        BoundCopyFrom analysis = analyze(
            "COPY users FROM '/some/distant/file.ext'");
        assertThat(analysis.inputFormat()).isEqualTo(FileUriCollectPhase.InputFormat.JSON);
    }

    @Test
    public void testCopyToFile() throws Exception {
        assertThatThrownBy(() -> analyze("COPY users TO '/blah.txt'"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Using COPY TO without specifying a DIRECTORY is not supported");
    }

    @Test
    public void testCopyToDirectory() throws Exception {
        BoundCopyTo analysis = analyze("COPY users TO DIRECTORY '/foo'");
        TableInfo tableInfo = analysis.table();
        assertThat(tableInfo.ident()).isEqualTo(USER_TABLE_IDENT);
        assertThat(analysis.uri()).isLiteral("/foo");
    }

    @Test
    public void testCopySysTableTo() throws Exception {
        assertThatThrownBy(() -> analyze("COPY sys.nodes TO DIRECTORY '/foo'"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessage(
                "The relation \"sys.nodes\" doesn't support or allow COPY TO " +
                "operations");
    }

    @Test
    public void testCopyToWithColumnList() throws Exception {
        BoundCopyTo analysis = analyze("COPY users (id, name) TO DIRECTORY '/tmp'");
        List<Symbol> outputs = analysis.outputs();
        assertThat(outputs).hasSize(2);
        assertThat(outputs.get(0)).isReference().hasName("_doc['id']");
        assertThat(outputs.get(1)).isReference().hasName("_doc['name']");
    }

    @Test
    public void testCopyToFileWithCompressionParams() throws Exception {
        BoundCopyTo analysis = analyze(
            "COPY users TO DIRECTORY '/blah' WITH (compression='gzip')");
        assertThat(analysis.table().ident()).isEqualTo(USER_TABLE_IDENT);

        assertThat(analysis.uri()).isLiteral("/blah");
        assertThat(analysis.compressionType()).isEqualTo(WriterProjection.CompressionType.GZIP);
    }

    @Test
    public void testCopyToFileWithPartitionedTable() throws Exception {
        BoundCopyTo analysis = analyze("COPY parted TO DIRECTORY '/blah'");
        assertThat(analysis.table().ident().name()).isEqualTo("parted");
        assertThat(analysis.overwrites()).hasSize(1);
    }

    @Test
    public void testCopyToFileWithPartitionClause() throws Exception {
        BoundCopyTo analysis = analyze(
            "COPY parted PARTITION (date=1395874800000) TO DIRECTORY '/blah'");
        String parted = new PartitionName(
            new RelationName("doc", "parted"), Collections.singletonList("1395874800000")).asIndexName();
        assertThat(analysis.whereClause().partitions()).containsExactly(parted);
    }

    @Test
    public void testCopyToDirectoryWithPartitionClause() throws Exception {
        BoundCopyTo analysis = analyze(
            "COPY parted PARTITION (date=1395874800000) TO DIRECTORY '/tmp'");
        String parted = new PartitionName(
            new RelationName("doc", "parted"), Collections.singletonList("1395874800000")).asIndexName();
        assertThat(analysis.whereClause().partitions()).containsExactly(parted);
        assertThat(analysis.overwrites()).isEmpty();
    }

    @Test
    public void testCopyToDirectoryWithNotExistingPartitionClause() throws Exception {
        assertThatThrownBy(() -> analyze("COPY parted PARTITION (date=0) TO DIRECTORY '/tmp/'"))
            .isExactlyInstanceOf(PartitionUnknownException.class)
            .hasMessage("No partition for table 'doc.parted' with ident '04130' exists");
    }

    @Test
    public void testCopyToWithWhereClause() throws Exception {
        BoundCopyTo analysis = analyze(
            "COPY parted WHERE id = 1 TO DIRECTORY '/tmp/foo'");
        assertThat(analysis.whereClause().query()).isFunction("op_=");
    }

    @Test
    public void testCopyToWithPartitionIdentAndPartitionInWhereClause() throws Exception {
        BoundCopyTo analysis = analyze(
            "COPY parted PARTITION (date=1395874800000) WHERE date = 1395874800000 TO DIRECTORY '/tmp/foo'");
        String parted = new PartitionName(
            new RelationName("doc", "parted"), Collections.singletonList("1395874800000")).asIndexName();
        assertThat(analysis.whereClause().partitions()).containsExactly(parted);
    }

    @Test
    public void testCopyToWithPartitionIdentAndWhereClause() throws Exception {
        BoundCopyTo analysis = analyze(
            "COPY parted PARTITION (date=1395874800000) WHERE id = 1 TO DIRECTORY '/tmp/foo'");
        String parted = new PartitionName(
            new RelationName("doc", "parted"), Collections.singletonList("1395874800000")).asIndexName();
        WhereClause where = analysis.whereClause();
        assertThat(where.partitions()).containsExactly(parted);
        assertThat(where.query()).isFunction("op_=");
    }

    @Test
    public void testCopyToWithNotExistingPartitionClause() throws Exception {
        assertThatThrownBy(() -> analyze("COPY parted PARTITION (date=0) TO DIRECTORY '/tmp/blah'"))
            .isExactlyInstanceOf(PartitionUnknownException.class)
            .hasMessage("No partition for table 'doc.parted' with ident '04130' exists");
    }

    @Test
    public void testCopyToFileWithSelectedColumnsAndOutputFormatParam() throws Exception {
        BoundCopyTo analysis = analyze(
            "COPY users (id, name) TO DIRECTORY '/blah' WITH (format='json_object')");
        assertThat(analysis.table().ident()).isEqualTo(USER_TABLE_IDENT);

        assertThat(analysis.uri()).isLiteral("/blah");
        assertThat(analysis.outputFormat()).isEqualTo(WriterProjection.OutputFormat.JSON_OBJECT);
        assertThat(analysis.outputNames()).containsExactly("id", "name");
    }

    @Test
    public void testCopyToFileWithUnsupportedOutputFormatParam() throws Exception {
        assertThatThrownBy(() -> analyze("COPY users TO DIRECTORY '/blah' WITH (format='json_array')"))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessage("Output format not supported without specifying columns.");
    }

    @Test
    public void testCopyFromWithReferenceAssignedToProperty() throws Exception {
        assertThatThrownBy(() -> analyze("COPY users FROM '/blah.txt' with (compression = gzip)"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage(
                "Columns cannot be used in this context." +
                " Maybe you wanted to use a string literal which requires single quotes: 'gzip'");
    }

    @Test
    public void testCopyFromFileUriArray() throws Exception {
        List<String> files = Arrays.asList("/f1.json", "/f2.json");
        BoundCopyFrom copyFrom = analyze(
            "COPY users FROM ?", new Object[]{files});
        assertThat(copyFrom.uri()).isLiteral(List.of("/f1.json", "/f2.json"));
    }

    @Test
    public void testCopyFromInvalidTypedExpression() throws Exception {
        Object[] files = $(1, 2, 3);
        assertThatThrownBy(() -> analyze("COPY users FROM ?", new Object[]{files}))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("fileUri must be of type STRING or STRING ARRAY. Got integer_array");
    }

    @Test
    public void testStringAsNodeFiltersArgument() throws Exception {
        assertThatThrownBy(() -> analyze("COPY users FROM '/' WITH (node_filters='invalid')"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage(
                "Invalid parameter passed to node_filters. " +
                "Expected an object with name or id keys and string values. Got 'invalid'"
            );
    }

    @Test
    public void testObjectWithWrongKeyAsNodeFiltersArgument() throws Exception {
        assertThatThrownBy(() -> analyze("COPY users FROM '/' WITH (node_filters={dummy='invalid'})"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid node_filters arguments: [dummy]");
    }

    @Test
    public void testObjectWithInvalidValueTypeAsNodeFiltersArgument() throws Exception {
        assertThatThrownBy(() -> analyze("COPY users FROM '/' WITH (node_filters={name=20})"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("node_filters argument 'name' must be a String, not 20 (Integer)");
    }

    @Test
    public void test_copy_to_using_upper_case_columns_does_not_result_in_quoted_col_names() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable("CREATE TABLE doc.upper (\"Name\" varchar)");
        BoundCopyTo analysis = analyze("COPY doc.upper (\"Name\") TO DIRECTORY '/dummy'");
        assertThat(analysis.outputNames()).containsExactly("Name");
    }

    @Test
    public void test_copy_to_using_generated_columns_does_not_result_in_full_expression() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable("CREATE TABLE doc.generated_copy (i as 1 + 1)");
        BoundCopyTo analysis = analyze("COPY doc.generated_copy (i) TO DIRECTORY '/dummy'");
        assertThat(analysis.outputNames()).containsExactly("i");
    }

    @Test
    public void test_cannot_use_return_summary_without_waiting_for_completion() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x int)");
        assertThatThrownBy(() -> analyze("copy tbl from '/*' with (wait_for_completion = false) return summary"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Cannot use RETURN SUMMARY with wait_for_completion=false. Either set wait_for_completion=true, or remove RETURN SUMMARY");
    }

    @Test
    public void test_non_deterministic_function_is_not_normalized() {
        AnalyzedCopyFrom analyzedCopyFrom = e.analyze("copy users from '/tmp/t_' || curdate()");
        assertThat(analyzedCopyFrom.uri()).isFunction(
            ConcatFunction.NAME,
            isLiteral("/tmp/t_"),
            isFunction(
                ImplicitCastFunction.NAME,
                isFunction(CurrentDateFunction.NAME),
                isLiteral("text")
            )
        );
        AnalyzedCopyTo analyzedCopyTo = e.analyze("copy users to directory '/tmp/' || curdate()");
        assertThat(analyzedCopyTo.uri()).isFunction(
            ConcatFunction.NAME,
            isLiteral("/tmp/"),
            isFunction(
                ImplicitCastFunction.NAME,
                isFunction(CurrentDateFunction.NAME),
                isLiteral("text")
            )
        );
    }
}
