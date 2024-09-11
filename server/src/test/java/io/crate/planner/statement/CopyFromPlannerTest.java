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

package io.crate.planner.statement;

import static io.crate.analyze.TableDefinitions.USER_TABLE_DEFINITION;
import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.AnalyzedCopyFrom;
import io.crate.analyze.BoundCopyFrom;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.execution.dsl.projection.SourceIndexWriterProjection;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.operators.SubQueryResults;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class CopyFromPlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private PlannerContext plannerContext;

    @Before
    public void setupExecutor() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable(USER_TABLE_DEFINITION)
            .addTable("create table t1 (a string, x int, i int)");
        plannerContext = e.getPlannerContext();
    }

    private Collect plan(String statement) {
        AnalyzedCopyFrom analysis = e.analyze(statement);
        BoundCopyFrom boundCopyFrom = CopyFromPlan.bind(
            analysis,
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            Row.EMPTY,
            SubQueryResults.EMPTY);
        return (Collect) CopyFromPlan.planCopyFromExecution(
            analysis,
            boundCopyFrom,
            clusterService.state().nodes(),
            plannerContext,
            Row.EMPTY,
            SubQueryResults.EMPTY
        );
    }

    @Test
    public void testCopyFromPlan() {
        Collect plan = plan("copy users from '/path/to/file.extension'");
        assertThat(plan.collectPhase()).isExactlyInstanceOf(FileUriCollectPhase.class);

        FileUriCollectPhase collectPhase = (FileUriCollectPhase) plan.collectPhase();
        assertThat(((Literal<?>) collectPhase.targetUri()).value()).isEqualTo("/path/to/file.extension");
    }

    public void testCopyFromPlanWithTargetColumns() {
        Collect plan = plan("copy users(id, name) from '/path/to/file.extension'");
        assertThat(plan.collectPhase()).isExactlyInstanceOf(FileUriCollectPhase.class);

        FileUriCollectPhase collectPhase = (FileUriCollectPhase) plan.collectPhase();
        assertThat(collectPhase.targetColumns()).containsExactly("id", "name");
    }

    @Test
    public void testCopyFromNumReadersSetting() {
        Collect plan = plan("copy users from '/path/to/file.extension' with (num_readers=1)");
        assertThat(plan.collectPhase()).isExactlyInstanceOf(FileUriCollectPhase.class);
        FileUriCollectPhase collectPhase = (FileUriCollectPhase) plan.collectPhase();
        assertThat(collectPhase.nodeIds()).hasSize(1);
    }

    @Test
    public void testCopyFromPlanWithParameters() {
        Collect collect = plan("copy users " +
                               "from '/path/to/file.ext' with (bulk_size=30, compression='gzip', shared=true, " +
                               "fail_fast=true, wait_for_completion=false)");
        assertThat(collect.collectPhase()).isExactlyInstanceOf(FileUriCollectPhase.class);

        FileUriCollectPhase collectPhase = (FileUriCollectPhase) collect.collectPhase();
        SourceIndexWriterProjection indexWriterProjection = (SourceIndexWriterProjection) collectPhase.projections().get(0);
        assertThat(indexWriterProjection.bulkActions()).isEqualTo(30);
        assertThat(collectPhase.compression()).isEqualTo("gzip");
        assertThat(collectPhase.sharedStorage()).isTrue();
        assertThat(indexWriterProjection.failFast()).isTrue();
        assertThat(collectPhase.withClauseOptions().getAsBoolean("wait_for_completion", true)).isFalse();

        // verify defaults:
        collect = plan("copy users from '/path/to/file.ext'");
        collectPhase = (FileUriCollectPhase) collect.collectPhase();
        indexWriterProjection = (SourceIndexWriterProjection) collectPhase.projections().get(0);
        assertThat(collectPhase.compression()).isNull();
        assertThat(collectPhase.sharedStorage()).isNull();
        assertThat(indexWriterProjection.failFast()).isFalse();
        assertThat(collectPhase.withClauseOptions()).isEqualTo(Settings.EMPTY);
    }

    @Test
    public void testIdIsNotCollectedOrUsedAsClusteredBy() {
        Collect collect = plan("copy t1 from '/path/file.ext'");
        SourceIndexWriterProjection projection =
            (SourceIndexWriterProjection) collect.collectPhase().projections().get(0);
        assertThat(projection.clusteredBy()).isNull();
        List<Symbol> toCollectSymbols = collect.collectPhase().toCollect();
        assertThat(toCollectSymbols).hasSize(1);
        assertThat(toCollectSymbols.get(0)).isInstanceOf(Reference.class);
        Reference refToCollect = (Reference) toCollectSymbols.get(0);
        assertThat(refToCollect.column().fqn()).isEqualTo("_raw");
    }

    @Test
    public void testCopyFromPlanWithInvalidParameters() {
        assertThatThrownBy(() -> plan("copy users from '/path/to/file.ext' with (bulk_size=-28)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Failed to parse value [-28] for setting [bulk_size] must be >= 1");
    }

    @Test
    public void testCopyFromPlanWithInvalidCompressionParameter() {
        assertThatThrownBy(() -> plan("copy users from '/path/to/file.ext' with (compression=true)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unsupported setting value: true. Supported values are: gzip");
    }

    @Test
    public void testNodeFiltersNoMatch() {
        Collect cm = plan("copy users from '/path' with (node_filters={name='foobar'})");
        assertThat(cm.collectPhase().nodeIds()).isEmpty();
    }

    @Test
    public void test_validation_is_not_supported() throws Exception {
        assertThatThrownBy(() -> plan("copy users from '/path' with (validation = false)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Setting 'validation' is not supported");
    }

    @Test
    public void test_num_readers_minimal_value_must_be_greater_than_0() {
        assertThatThrownBy(
            () -> plan("copy users from '/path/to/file.extension' with (num_readers = 0)")
        ).isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("Failed to parse value [0] for setting [num_readers] must be >= 1");
    }

    @Test
    public void copy_from_protocol_in_with_clause_is_not_rejected() {
        // We do unknown or irrelevant property validation for "file" scheme in the server module.
        // Verify that properties of a non-file scheme are not rejected.
        // They are supposed to be validated later in a plugin, implementing the scheme.
        Collect collect = plan("COPY users FROM 's3://bucket' WITH (protocol='http')");
        assertThat(collect.collectPhase()).isExactlyInstanceOf(FileUriCollectPhase.class);
        FileUriCollectPhase collectPhase = (FileUriCollectPhase) collect.collectPhase();
        assertThat(collectPhase.withClauseOptions().get("protocol")).isEqualTo("http");
    }

    @Test
    public void copy_from_file_scheme_unknown_property_in_with_clause_is_rejected() {
        assertThatThrownBy(() -> plan("COPY users FROM '/some/distant/file.ext' WITH (dummy='dummy')"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Setting 'dummy' is not supported");
    }
}
