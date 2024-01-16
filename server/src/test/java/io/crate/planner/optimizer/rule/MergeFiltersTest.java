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

package io.crate.planner.optimizer.rule;

import static io.crate.testing.Asserts.assertThat;

import java.util.Collections;
import java.util.Map;
import java.util.function.UnaryOperator;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.RelationName;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.Filter;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Match;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;

public class MergeFiltersTest extends CrateDummyClusterServiceUnitTest {

    private SqlExpressions e;
    private AbstractTableRelation<?> tr1;
    private PlanStats planStats;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Map<RelationName, AnalyzedRelation> sources = T3.sources(clusterService);
        e = new SqlExpressions(sources);
        planStats = new PlanStats(e.nodeCtx, CoordinatorTxnCtx.systemTransactionContext(), new TableStats());
        tr1 = (AbstractTableRelation<?>) sources.get(T3.T1);

    }

    @Test
    public void testMergeFiltersMatchesOnAFilterWithAnotherFilterAsChild() {
        Collect source = new Collect(tr1, Collections.emptyList(), WhereClause.MATCH_ALL);
        Filter sourceFilter = new Filter(source, e.asSymbol("x > 10"));
        Filter parentFilter = new Filter(sourceFilter, e.asSymbol("y > 10"));

        MergeFilters mergeFilters = new MergeFilters();
        Match<Filter> match = mergeFilters.pattern().accept(parentFilter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isSameAs(parentFilter);

        Filter mergedFilter = mergeFilters.apply(match.value(),
                                                 match.captures(),
                                                 planStats,
                                                 CoordinatorTxnCtx.systemTransactionContext(),
                                                 e.nodeCtx,
                                                 UnaryOperator.identity());
        assertThat(mergedFilter.query()).isSQL("((doc.t2.y > 10) AND (doc.t1.x > 10))");
    }
}
