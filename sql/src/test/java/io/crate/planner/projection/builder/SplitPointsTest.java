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
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.hamcrest.Matchers;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.*;
import static org.junit.Assert.assertThat;

public class SplitPointsTest {

    @Test
    public void testSplitPointsCreationWithFunctionInAggregation() throws Exception {
        SQLExecutor e = SQLExecutor.builder(new NoopClusterService()).addDocTable(T3.T1_INFO).build();

        AnalyzedStatement analyze = e.analyze("select sum(coalesce(x, 0::integer)) + 10 from t1");
        QueriedRelation relation = ((SelectAnalyzedStatement) analyze).relation();

        SplitPoints splitPoints = SplitPoints.create(relation.querySpec());

        //noinspection unchecked
        assertThat(splitPoints.leaves(), Matchers.contains(isReference("x"), isLiteral(0)));
        assertThat(splitPoints.toCollect(), Matchers.contains(isFunction("coalesce")));
        assertThat(splitPoints.aggregates(), Matchers.contains(isFunction("sum")));
    }
}
