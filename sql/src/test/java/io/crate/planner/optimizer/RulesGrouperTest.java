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

package io.crate.planner.optimizer;

import io.crate.analyze.OrderBy;
import io.crate.expression.symbol.Literal;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.Limit;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.planner.operators.Order;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;

public class RulesGrouperTest extends CrateUnitTest {

    @Test
    public void testWithLogicalPlanHierarchy() {
        Rule orderRule = new NoOpRule(Pattern.typeOf(Order.class));
        Rule otherOrderRule = new NoOpRule(Pattern.typeOf(Order.class));
        Rule filterRule = new NoOpRule(Pattern.typeOf(Filter.class));
        Rule limitRule = new NoOpRule(Pattern.typeOf(Limit.class));

        RulesGrouper rulesGrouper = RulesGrouper.builder()
            .register(orderRule)
            .register(otherOrderRule)
            .register(filterRule)
            .register(limitRule)
            .build();

        Filter filter = new Filter(mock(LogicalPlan.class), Literal.BOOLEAN_TRUE);
        Order order = new Order(filter, mock(OrderBy.class));

        assertThat(
            rulesGrouper.getCandidates(filter).collect(toSet()),
            contains(filterRule));
        assertThat(
            rulesGrouper.getCandidates(order).collect(toSet()),
            containsInAnyOrder(orderRule, otherOrderRule));
    }

    @Test
    public void testInterfacesHierarchy() {
        Rule a = new NoOpRule(Pattern.typeOf(A.class));
        Rule b = new NoOpRule(Pattern.typeOf(B.class));
        Rule ab = new NoOpRule(Pattern.typeOf(AB.class));

        RulesGrouper rulesGrouper = RulesGrouper.builder()
            .register(a)
            .register(b)
            .register(ab)
            .build();

        assertThat(
            rulesGrouper.getCandidates(new A() {}).collect(toSet()),
            contains(a));
        assertThat(
            rulesGrouper.getCandidates(new B() {}).collect(toSet()),
            contains(b));
        assertThat(
            rulesGrouper.getCandidates(new AB()).collect(toSet()),
            containsInAnyOrder(ab, a, b));
    }

    private static class NoOpRule implements Rule<LogicalPlanner> {
        private final Pattern pattern;

        private NoOpRule(Pattern pattern) {
            this.pattern = pattern;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Pattern pattern() {
            return pattern;
        }

        @Override
        public LogicalPlan apply(LogicalPlanner plan, Captures captures) {
            return null;
        }

        @Override
        public String toString() {
            return toStringHelper(this)
                .add("pattern", pattern)
                .toString();
        }
    }

    private interface A {}

    private interface B {}

    private static class AB
        implements A, B {}
}
