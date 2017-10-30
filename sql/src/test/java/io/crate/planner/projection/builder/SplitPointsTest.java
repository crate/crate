/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.projection.builder;

import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import org.hamcrest.Matchers;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isReference;
import static org.hamcrest.Matchers.contains;

public class SplitPointsTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testSplitPointsCreationWithFunctionInAggregation() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService).addDocTable(T3.T1_INFO).build();

        AnalyzedStatement analyze = e.analyze("select sum(coalesce(x, 0::integer)) + 10 from t1");
        QueriedRelation relation = ((SelectAnalyzedStatement) analyze).relation();

        SplitPoints splitPoints = SplitPoints.create(relation);

        //noinspection unchecked
        assertThat(splitPoints.toCollect(), contains(isFunction("coalesce")));
        assertThat(splitPoints.aggregates(), contains(isFunction("sum")));
    }

    @Test
    public void testSplitPointsCreationSelectItemAggregationsAreAlwaysAdded() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService).addDocTable(T3.T1_INFO).build();

        AnalyzedStatement analyze = e.analyze("select sum(coalesce(x, 0::integer)), sum(coalesce(x, 0::integer)) + 10 from t1");
        QueriedRelation relation = ((SelectAnalyzedStatement) analyze).relation();

        SplitPoints splitPoints = SplitPoints.create(relation);

        //noinspection unchecked
        assertThat(splitPoints.toCollect(), contains(isFunction("coalesce")));
        //noinspection unchecked
        assertThat(splitPoints.aggregates(), contains(isFunction("sum")));
    }


    @Test
    public void testScalarIsNotCollectedEarly() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService).addDocTable(T3.T1_INFO).build();
        AnalyzedStatement analyze = e.analyze("select x + 1 from t1 group by x");
        QueriedRelation relation = ((SelectAnalyzedStatement) analyze).relation();

        SplitPoints splitPoints = SplitPoints.create(relation);
        assertThat(splitPoints.toCollect(), contains(isReference("x")));
        assertThat(splitPoints.aggregates(), Matchers.emptyIterable());
    }
}
