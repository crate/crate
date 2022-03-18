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

import io.crate.analyze.AnalyzedCopyFrom;
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
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static io.crate.analyze.TableDefinitions.USER_TABLE_DEFINITION;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class CopyFromPlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private PlannerContext plannerContext;

    @Before
    public void setupExecutor() throws IOException {
        e = SQLExecutor.builder(clusterService)
            .addTable(USER_TABLE_DEFINITION)
            .addTable("create table t1 (a string, x int, i int)")
            .build();
        plannerContext = e.getPlannerContext(clusterService.state());
    }

    private Collect plan(String statement) {
        AnalyzedCopyFrom analysis = e.analyze(statement);
        return (Collect) CopyFromPlan.planCopyFromExecution(
            analysis,
            clusterService.state().nodes(),
            plannerContext,
            Row.EMPTY,
            SubQueryResults.EMPTY
        );
    }

    @Test
    public void testCopyFromPlan() {
        Collect plan = plan("copy users from '/path/to/file.extension'");
        assertThat(plan.collectPhase(), instanceOf(FileUriCollectPhase.class));

        FileUriCollectPhase collectPhase = (FileUriCollectPhase) plan.collectPhase();
        assertThat(((Literal) collectPhase.targetUri()).value(), is("/path/to/file.extension"));
    }

    public void testCopyFromPlanWithTargetColumns() {
        Collect plan = plan("copy users(id, name) from '/path/to/file.extension'");
        assertThat(plan.collectPhase(), instanceOf(FileUriCollectPhase.class));

        FileUriCollectPhase collectPhase = (FileUriCollectPhase) plan.collectPhase();
        assertThat(collectPhase.targetColumns(), is(List.of("id", "name")));
    }

    @Test
    public void testCopyFromNumReadersSetting() {
        Collect plan = plan("copy users from '/path/to/file.extension' with (num_readers=1)");
        assertThat(plan.collectPhase(), instanceOf(FileUriCollectPhase.class));
        FileUriCollectPhase collectPhase = (FileUriCollectPhase) plan.collectPhase();
        assertThat(collectPhase.nodeIds().size(), is(1));
    }

    @Test
    public void testCopyFromPlanWithParameters() {
        Collect collect = plan("copy users " +
                               "from '/path/to/file.ext' with (bulk_size=30, compression='gzip', shared=true, " +
                               "fail_fast=true, protocol='http', wait_for_completion=false)");
        assertThat(collect.collectPhase(), instanceOf(FileUriCollectPhase.class));

        FileUriCollectPhase collectPhase = (FileUriCollectPhase) collect.collectPhase();
        SourceIndexWriterProjection indexWriterProjection = (SourceIndexWriterProjection) collectPhase.projections().get(0);
        assertThat(indexWriterProjection.bulkActions(), is(30));
        assertThat(collectPhase.compression(), is("gzip"));
        assertThat(collectPhase.sharedStorage(), is(true));
        assertThat(indexWriterProjection.failFast(), is(true));
        assertThat(collectPhase.withClauseOptions().get("protocol"), is("http"));
        assertThat(collectPhase.withClauseOptions().getAsBoolean("wait_for_completion", true), is(false));

        // verify defaults:
        collect = plan("copy users from '/path/to/file.ext'");
        collectPhase = (FileUriCollectPhase) collect.collectPhase();
        indexWriterProjection = (SourceIndexWriterProjection) collectPhase.projections().get(0);
        assertThat(collectPhase.compression(), is(nullValue()));
        assertThat(collectPhase.sharedStorage(), is(nullValue()));
        assertThat(indexWriterProjection.failFast(), is(false));
        assertThat(collectPhase.withClauseOptions(), is(Settings.EMPTY));
    }

    @Test
    public void testIdIsNotCollectedOrUsedAsClusteredBy() {
        Collect collect = plan("copy t1 from '/path/file.ext'");
        SourceIndexWriterProjection projection =
            (SourceIndexWriterProjection) collect.collectPhase().projections().get(0);
        assertThat(projection.clusteredBy(), is(nullValue()));
        List<Symbol> toCollectSymbols = collect.collectPhase().toCollect();
        assertThat(toCollectSymbols.size(), is(1));
        assertThat(toCollectSymbols.get(0), instanceOf(Reference.class));
        Reference refToCollect = (Reference) toCollectSymbols.get(0);
        assertThat(refToCollect.column().fqn(), is("_raw"));
    }

    @Test
    public void testCopyFromPlanWithInvalidParameters() {
        expectedException.expect(IllegalArgumentException.class);
        plan("copy users from '/path/to/file.ext' with (bulk_size=-28)");
    }

    @Test
    public void testNodeFiltersNoMatch() {
        Collect cm = plan("copy users from '/path' with (node_filters={name='foobar'})");
        assertThat(cm.collectPhase().nodeIds().size(), is(0));
    }
}
