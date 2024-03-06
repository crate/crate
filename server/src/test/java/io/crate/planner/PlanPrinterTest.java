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

package io.crate.planner;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;

public class PlanPrinterTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void setUpExecutor() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T2_DEFINITION);
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
                is("{Collect={" +
                "collectPhase={COLLECT={" +
                    "distribution={distributedByColumn=0, type=BROADCAST}, executionNodes=[n1], id=0, " +

                    "projections=[" +
                        "{keys=INPUT(1), type=HashAggregation, aggregations=max(INPUT(0))}, " +
                        "{keys=INPUT(0), type=HashAggregation, aggregations=max(INPUT(1))}, " +
                        "{filter=(INPUT(1) > 10), type=Filter}, " +
                        "{outputs=INPUT(0), INPUT(1), offset=0, limit=-1, orderBy=[INPUT(0) ASC], type=OrderLimitAndOffset}], " +
                    "routing={n1={t1=[0, 1, 2, 3]}}, " +
                    "toCollect=[x, a], type=executionPhase, where=true}}, " +
                    "type=executionPlan}}")
        );
    }

    @Test
    public void testNestedLoopJoin() {
        Map<String, Object> map = printPlan("select t1.x, t2.y " +
                                            "from t1, t2 " +
                                            "where t1.x > 10 and t2.y < 10 " +
                                            "order by 1");
        String mapStr = map.toString();
        assertThat(mapStr, containsString("{Join={joinPhase={NESTED_LOOP={distribution={distributed"));
    }

    @Test
    public void testHashJoin() {
        Map<String, Object> map = printPlan("select t1.x, t2.y " +
                                            "from t1 inner join t2 " +
                                            "on t1.x = t2.y " +
                                            "order by 1");
        assertThat(map.toString(),containsString("{Join={joinPhase={HASH_JOIN={distribution={distributedBy"));
    }

    @Test
    public void testUnion() {
        Map<String, Object> map = printPlan("select x from t1 " +
                                            "union all " +
                                            "select y from t2 " +
                                            "order by 1 " +
                                            "limit 10");
        assertThat(map.toString(),
            is("{UnionExecutionPlan={" +
               "left={Collect={" +
                    "collectPhase={COLLECT={distribution={distributedByColumn=0, type=BROADCAST}, " +
                    "executionNodes=[n1], id=0, orderBy=x ASC, " +
                    "routing={n1={t1=[0, 1, 2, 3]}}, " +
                    "toCollect=[x], " +
                    "type=executionPhase, " +
                    "where=true}}, " +
                    "type=executionPlan}}, " +
               "mergePhase={MERGE={" +
                    "distribution={distributedByColumn=0, type=BROADCAST}, " +
                    "executionNodes=[n1], id=2, " +
                    "projections=[{outputs=INPUT(0), offset=0, limit=10, type=LimitAndOffset}], " +
                    "type=executionPhase}}, " +
               "right={Collect={" +
                    "collectPhase={COLLECT={distribution={distributedByColumn=0, type=BROADCAST}, " +
                    "executionNodes=[n1], id=1, orderBy=y ASC, " +
                    "routing={n1={t2=[0, 1, 2, 3]}}, " +
                    "toCollect=[y], " +
                    "type=executionPhase, where=true}}, " +
                    "type=executionPlan}}, " +
               "type=executionPlan}}"));
    }

    @Test
    public void testCountPlan() {
        Map<String, Object> map = printPlan("select count(*) " +
                                            "from t1 " +
                                            "where t1.x > 10");
        assertThat(map.toString(),
            is("{CountPlan={type=executionPlan}, " +
               "countPhase={COUNT={executionNodes=[n1], id=0, type=executionPhase}, " +
                   "distribution={distributedByColumn=0, type=BROADCAST}, " +
                   "routing={n1={t1=[0, 1, 2, 3]}}, " +
                   "where=(x > 10)}, " +
               "mergePhase={MERGE={distribution={distributedByColumn=0, type=BROADCAST}, " +
                    "executionNodes=[n1], id=1, " +
                    "projections=[{type=MERGE_COUNT_AGGREGATION}], type=executionPhase}}}"));
    }
}
