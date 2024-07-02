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

package io.crate.execution.engine.window;

import static io.crate.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.crate.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
import static io.crate.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.TableDefinitions;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.WindowAggProjection;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.WindowFunction;
import io.crate.planner.node.dql.Collect;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class WindowDefinitionTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION);
    }

    @Test
    public void testStartUnboundedFollowingIsIllegal() {
        assertThatThrownBy(() -> e.analyze(
                "select sum(unnest) over(RANGE BETWEEN UNBOUNDED FOLLOWING and CURRENT ROW) FROM " +
                    "unnest([1, 2, 1, 1, 1, 4])"))
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessage("Frame start cannot be UNBOUNDED_FOLLOWING");
    }

    @Test
    public void testStartFollowingIsIllegal() {
        assertThatThrownBy(() -> e.analyze(
                "select sum(unnest) over(RANGE BETWEEN 1 FOLLOWING and CURRENT ROW) FROM " +
                    "unnest([1, 2, 1, 1, 1, 4])"))
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessage("Frame start cannot be FOLLOWING");
    }

    @Test
    public void testEndPrecedingIsIllegal() {
        assertThatThrownBy(() -> e.analyze(
                "select sum(unnest) over(RANGE BETWEEN CURRENT ROW and 1 PRECEDING) FROM " +
                    "unnest([1, 2, 1, 1, 1, 4])"))
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessage("Frame end cannot be PRECEDING");
    }

    @Test
    public void testEndUnboundedPrecedingIsIllegal() {
        assertThatThrownBy(() -> e.analyze(
            "select sum(unnest) over(RANGE BETWEEN CURRENT ROW and UNBOUNDED PRECEDING) FROM " +
                "unnest([1, 2, 1, 1, 1, 4])"))
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessage("Frame end cannot be UNBOUNDED_PRECEDING");
    }

    @Test
    public void testFrameEndDefaultsToCurrentRowIfNotSpecified() {
        AnalyzedRelation analyze = e.analyze(
            "select sum(unnest) over(RANGE UNBOUNDED PRECEDING) FROM " +
            "unnest([1, 2, 1, 1, 1, 4])");
        assertThat(analyze).isExactlyInstanceOf(QueriedSelectRelation.class);
        List<Symbol> outputs = analyze.outputs();
        assertThat(outputs).hasSize(1);
        WindowFunction windowFunction = (WindowFunction) outputs.get(0);
        assertThat(windowFunction.windowDefinition().windowFrameDefinition().end().type()).isEqualTo(CURRENT_ROW);
    }

    @Test
    public void testUnboundedPrecedingUnboundedFollowingFrameIsAllowed() {
        Collect collect = e.plan(
            "select sum(unnest) over(RANGE BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) FROM " +
            "unnest([1, 2, 1, 1, 1, 4])");
        List<Projection> projections = collect.collectPhase().projections();
        assertThat(projections).hasSize(2);
        WindowAggProjection windowProjection = null;
        for (Projection projection : projections) {
            if (projection instanceof WindowAggProjection) {
                windowProjection = (WindowAggProjection) projection;
                break;
            }
        }
        assertThat(windowProjection).isNotNull();
        List<? extends Symbol> outputs = windowProjection.outputs();
        assertThat(outputs).hasSize(2); // IC and window function
        WindowFunction windowFunction = null;
        for (Symbol output : outputs) {
            if (output instanceof WindowFunction) {
                windowFunction = (WindowFunction) output;
            }
        }
        assertThat(windowFunction).isNotNull();
        assertThat(windowFunction.windowDefinition().windowFrameDefinition().start().type()).isEqualTo(UNBOUNDED_PRECEDING);
        assertThat(windowFunction.windowDefinition().windowFrameDefinition().end().type()).isEqualTo(UNBOUNDED_FOLLOWING);
    }
}
