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

package io.crate.planner;

import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.projection.*;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.instanceOf;


public class SubQueryPlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void setUpExecutor() throws Exception {
         e = SQLExecutor.builder(clusterService).addDocTable(T3.T1_INFO).build();
    }

    @Test
    public void testNestedSimpleSelectUsesFetch() throws Exception {
        QueryThenFetch qtf = e.plan(
            "select x, i from (select x, i from t1 order by x asc limit 10) ti order by x desc limit 3");
        Collect collect = (Collect) qtf.subPlan();
        assertThat(collect.collectPhase().projections(), Matchers.contains(
            instanceOf(TopNProjection.class),
            instanceOf(TopNProjection.class),
            // TODO: We can optimize this to delay fetch until after the OrderedTopNProjection
            instanceOf(FetchProjection.class),
            instanceOf(OrderedTopNProjection.class)
        ));
    }

    @Test
    public void testNestedSimpleSelectContainsFilterProjectionForWhereClause() throws Exception {
        QueryThenFetch qtf = e.plan("select x, i from " +
                                    "   (select x, i from t1 order by x asc limit 10) ti " +
                                    "where ti.x = 10 " +
                                    "order by x desc limit 3");
        List<Projection> projections = ((Collect) qtf.subPlan()).collectPhase().projections();
        assertThat(projections, Matchers.hasItem(instanceOf(FilterProjection.class)));
    }
}
