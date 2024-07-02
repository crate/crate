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

import static io.crate.testing.Asserts.assertThat;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.TableDefinitions;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.WriterProjection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Merge;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.operators.SubQueryResults;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class CopyToPlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.builder(clusterService)
            .setNumNodes(2)
            .build()
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .addPartitionedTable(
                "create table parted (" +
                "   id int," +
                "   name string," +
                "   date timestamp with time zone," +
                "   obj object" +
                ") partitioned by (date) ",
                new PartitionName(new RelationName("doc", "parted"), singletonList("1395874800000")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted"), singletonList("1395961200000")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted"), singletonList(null)).asIndexName()
            )
            .addPartitionedTable(
                "create table parted_generated (" +
                "   ts timestamp with time zone," +
                "   day as date_trunc('day', ts)" +
                ") partitioned by (day) ",
                new PartitionName(new RelationName("doc", "parted_generated"), List.of("1395874800000")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted_generated"), List.of("1395961200000")).asIndexName()
            );
    }

    private <T> T plan(String stmt) {
        CopyToPlan plan = e.plan(stmt);
        var boundedCopyTo = CopyToPlan.bind(
            plan.copyTo(),
            e.getPlannerContext().transactionContext(),
            e.getPlannerContext().nodeContext(),
            Row.EMPTY,
            SubQueryResults.EMPTY,
            e.getPlannerContext().clusterState().metadata()
        );
        //noinspection unchecked
        return (T) CopyToPlan.planCopyToExecution(
            mock(DependencyCarrier.class),
            boundedCopyTo,
            e.getPlannerContext(),
            e.planStats(),
            new ProjectionBuilder(e.nodeCtx),
            Row.EMPTY,
            SubQueryResults.EMPTY);
    }

    @Test
    public void testCopyToWithColumnsReferenceRewrite() {
        Merge plan = plan("copy users (name) to directory '/tmp'");
        Collect innerPlan = (Collect) plan.subPlan();
        RoutedCollectPhase node = ((RoutedCollectPhase) innerPlan.collectPhase());
        Reference nameRef = (Reference) node.toCollect().getFirst();

        assertThat(nameRef.column().name()).isEqualTo(DocSysColumns.DOC.name());
        assertThat(nameRef.column().path().getFirst()).isEqualTo("name");
    }

    @Test
    public void testCopyToWithPartitionedGeneratedColumn() {
        // test that generated partition column is NOT exported
        Merge plan = plan("copy parted_generated to directory '/tmp'");
        Collect innerPlan = (Collect) plan.subPlan();
        RoutedCollectPhase node = ((RoutedCollectPhase) innerPlan.collectPhase());
        WriterProjection projection = (WriterProjection) node.projections().getFirst();
        assertThat(projection.overwrites()).isEmpty();
    }

    @Test
    public void testCopyToWithPartitionInWhereClauseRoutesToPartitionIndexOnly() {
        Merge merge = plan(
            "copy parted where date = 1395874800000 to directory '/tmp/foo'");
        Collect collect = (Collect) merge.subPlan();
        String expectedIndex = new PartitionName(
            new RelationName("doc", "parted"), singletonList("1395874800000")).asIndexName();

        assertThat(
            ((RoutedCollectPhase) collect.collectPhase()).routing().locations().values().stream()
                .flatMap(shardsByIndices -> shardsByIndices.keySet().stream())
                .collect(Collectors.toSet()))
            .containsExactly(expectedIndex);
    }

    @Test
    public void testCopyToWithInvalidPartitionInWhereClause() {
        assertThatThrownBy(
            () -> plan("copy parted partition (date=1395874800000) where date = 1395961200000 to directory '/tmp/foo'"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Given partition ident does not match partition evaluated from where clause");
    }

    @Test
    public void testCopyToPlanWithParameters() {
        Merge merge = plan("copy users to directory '/path/to' with (protocol='http', wait_for_completion=false)");
        Collect collect = (Collect) merge.subPlan();
        WriterProjection writerProjection = (WriterProjection) collect.collectPhase().projections().getFirst();
        assertThat(writerProjection.withClauseOptions().get("protocol")).isEqualTo("http");
        assertThat(writerProjection.withClauseOptions().getAsBoolean(
            "wait_for_completion", true)).isFalse();

        // null or empty compression
        for (var compression : List.of("''", "null")) {
            merge = plan(
                "copy users to directory '/path/to' with (compression=" + compression + ")");
            collect = (Collect) merge.subPlan();
            writerProjection = (WriterProjection) collect.collectPhase().projections().getFirst();
            assertThat(writerProjection.withClauseOptions()).hasSize(1);
            if (compression.equals("''")) {
                assertThat(writerProjection.withClauseOptions().get("compression")).isEmpty();
            } else {
                assertThat(writerProjection.withClauseOptions().get("compression")).isNull();
            }
        }

        // verify defaults:
        merge = plan("copy users to directory '/path/to/'");
        collect = (Collect) merge.subPlan();
        writerProjection = (WriterProjection) collect.collectPhase().projections().getFirst();
        assertThat(writerProjection.withClauseOptions()).isEqualTo(Settings.EMPTY);
    }
}
