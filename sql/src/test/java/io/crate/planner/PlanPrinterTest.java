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

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class PlanPrinterTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void setUpExecutor() throws IOException {
        e = SQLExecutor.builder(clusterService).enableDefaultTables().build();
    }

    private Map<String, Object> printPlan(String stmt) {
        return PlanPrinter.objectMap(e.plan(stmt));
    }

    @Test
    public void testGroupBy() {
        Map<String, Object> map = printPlan("select a, max(x) " +
                                            "from t1 " +
                                            "group by a " +
                                            "having max(x) > 10 " +
                                            "order by 1");
        assertThat(map.toString(),
            is("{Collect={type=executionPlan, " +
               "collectPhase={COLLECT={type=executionPhase, id=0, executionNodes=[n1], " +
                   "distribution={distributedByColumn=0, type=BROADCAST}, toCollect=Ref{doc.t1.x, integer}, Ref{doc.t1.a, text}, " +
                   "projections=[" +
                       "{type=HashAggregation, keys=IC{1, text}, aggregations=Aggregation{max, args=[IC{0, integer}], filter=true}}, " +
                       "{type=HashAggregation, keys=IC{0, text}, aggregations=Aggregation{max, args=[IC{1, integer}], filter=true}}, " +
                       "{type=Filter, filter=IC{1, integer} > 10}, " +
                       "{type=OrderByTopN, limit=-1, offset=0, outputs=IC{0, text}, IC{1, integer}, orderBy=[IC{0, text} ASC]}], " +
                    "routing={n1={t1=[0, 1, 2, 3]}}, where=true}}}}"));
    }

    @Test
    public void testNestedLoopJoin() {
        Map<String, Object> map = printPlan("select t1.x, t2.y " +
                                            "from t1, t2 " +
                                            "where t1.x > 10 and t2.y < 10 " +
                                            "order by 1");
        String mapStr = map.toString();
        assertThat(mapStr, containsString("joinPhase={NESTED_LOOP={type=executionPhase"));
    }

    @Test
    public void testHashJoin() {
        Map<String, Object> map = printPlan("select t1.x, t2.y " +
                                            "from t1 inner join t2 " +
                                            "on t1.x = t2.y " +
                                            "order by 1");
        assertThat(map.toString(),containsString("joinPhase={HASH_JOIN={type=executionPhas"));
    }

    @Test
    public void testUnion() {
        Map<String, Object> map = printPlan("select x from t1 " +
                                            "union all " +
                                            "select y from t2 " +
                                            "order by 1 " +
                                            "limit 10");
        assertThat(map.toString(),
            is("{UnionExecutionPlan={type=executionPlan, " +
               "left={Collect={type=executionPlan, " +
                   "collectPhase={COLLECT={type=executionPhase, id=0, executionNodes=[n1], " +
                       "distribution={distributedByColumn=0, type=BROADCAST}, toCollect=Ref{doc.t1.x, integer}, " +
                       "routing={n1={t1=[0, 1, 2, 3]}}, where=true, orderBy=Ref{doc.t1.x, integer} ASC}}}}, " +
               "right={Collect={type=executionPlan, " +
                   "collectPhase={COLLECT={type=executionPhase, id=1, executionNodes=[n1], " +
                   "distribution={distributedByColumn=0, type=BROADCAST}, toCollect=Ref{doc.t2.y, integer}, " +
                   "routing={n1={t2=[0, 1, 2, 3]}}, where=true, orderBy=Ref{doc.t2.y, integer} ASC}}}}, " +
               "mergePhase={MERGE={type=executionPhase, id=2, executionNodes=[n1], " +
                   "distribution={distributedByColumn=0, type=BROADCAST}, " +
                   "projections=[{type=TopN, limit=10, offset=0, outputs=IC{0, integer}}]}}}}"));
    }

    @Test
    public void testCountPlan() {
        Map<String, Object> map = printPlan("select count(*) " +
                                            "from t1 " +
                                            "where t1.x > 10");
        assertThat(map.toString(),
            is("{CountPlan={type=executionPlan}, " +
               "countPhase={COUNT={type=executionPhase, id=0, executionNodes=[n1]}, " +
                   "distribution={distributedByColumn=0, type=BROADCAST}, " +
                   "routing={n1={t1=[0, 1, 2, 3]}}, " +
                   "where=Ref{doc.t1.x, integer} > 10}, " +
               "mergePhase={MERGE={type=executionPhase, id=1, executionNodes=[n1], " +
                   "distribution={distributedByColumn=0, type=BROADCAST}, " +
               "projections=[{type=MERGE_COUNT_AGGREGATION}]}}}"));
    }
}
