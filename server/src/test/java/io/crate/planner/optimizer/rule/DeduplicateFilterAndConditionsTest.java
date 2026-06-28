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

import org.junit.Before;
import org.junit.Test;

import io.crate.expression.symbol.Symbol;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Match;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class DeduplicateFilterAndConditionsTest extends CrateDummyClusterServiceUnitTest {
    private LogicalPlan t1;
    private SQLExecutor e;

    @Before
    public void prepare() throws Exception {
        e = SQLExecutor.of(clusterService).addTable("create table t1 (a int, b int)");

        t1 = e.logicalPlan("SELECT a FROM t1");
    }

    @Test
    public void test_deduplicates_identical_predicates_joined_by_and() {
        Symbol predicates = e.asSymbol("a > 2 AND a > 2 AND a < 10");
        Filter filter = new Filter(t1, predicates);

        assertThat(filter).hasOperators(
                "Filter[(((a > 2) AND (a > 2)) AND (a < 10))]",
                "  └ Collect[doc.t1 | [a] | true]");

        DeduplicateFilterAndConditions rule = new DeduplicateFilterAndConditions();
        Match<Filter> match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isSameAs(filter);

        Filter deduplicatedFilter = rule.apply(match.value(), match.captures(), e.ruleContext());
        assertThat(deduplicatedFilter).hasOperators(
                "Filter[((a > 2) AND (a < 10))]",
                "  └ Collect[doc.t1 | [a] | true]");
    }

    @Test
    public void test_does_nothing_if_no_duplicates() {
        Symbol predicates = e.asSymbol("a > 2 AND a < 10");
        Filter filter = new Filter(t1, predicates);

        assertThat(filter).hasOperators(
                "Filter[((a > 2) AND (a < 10))]",
                "  └ Collect[doc.t1 | [a] | true]");

        DeduplicateFilterAndConditions rule = new DeduplicateFilterAndConditions();
        Match<Filter> match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isSameAs(filter);

        Filter deduplicatedFilter = rule.apply(match.value(), match.captures(), e.ruleContext());
        assertThat(deduplicatedFilter).isNull();
    }

    @Test
    public void test_does_nothing_if_no_AND_operator() {
        Symbol predicates = e.asSymbol("a = 20 OR a = 20");
        Filter filter = new Filter(t1, predicates);

        assertThat(filter).hasOperators(
                "Filter[((a = 20) OR (a = 20))]",
                "  └ Collect[doc.t1 | [a] | true]");

        DeduplicateFilterAndConditions rule = new DeduplicateFilterAndConditions();
        Match<Filter> match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isSameAs(filter);

        Filter deduplicatedFilter = rule.apply(match.value(), match.captures(), e.ruleContext());
        assertThat(deduplicatedFilter).isNull();
    }

    @Test
    public void test_deduplicates_and_preserves_other_conjunctions() {
        Symbol predicates = e.asSymbol("a = 20 AND a = 20 OR b = 10");
        Filter filter = new Filter(t1, predicates);

        assertThat(filter).hasOperators(
                "Filter[(((a = 20) AND (a = 20)) OR (b = 10))]",
                "  └ Collect[doc.t1 | [a] | true]");

        DeduplicateFilterAndConditions rule = new DeduplicateFilterAndConditions();
        Match<Filter> match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isSameAs(filter);

        Filter deduplicatedFilter = rule.apply(match.value(), match.captures(), e.ruleContext());
        assertThat(deduplicatedFilter).hasOperators(
                "Filter[((a = 20) OR (b = 10))]",
                "  └ Collect[doc.t1 | [a] | true]");
    }

}
