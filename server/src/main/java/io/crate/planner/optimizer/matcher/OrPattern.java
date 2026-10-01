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

package io.crate.planner.optimizer.matcher;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import io.crate.planner.operators.LogicalPlan;

/**
 * Matches if either the first or the second pattern matches; the first one is tried first.
 *
 * <pre>
 * typeOf(JoinPlan.class)
 *     .with(...)              // refines JoinPlan
 *     .or()
 *     .typeOf(Filter.class)
 *     .with(...)              // refines Filter only
 * </pre>
 *
 * Every {@code with} after {@code or().typeOf(...)} refines the second alternative.
 */
public final class OrPattern<T1, T2 extends LogicalPlan> extends Pattern<LogicalPlan> {

    private final Pattern<T1> first;
    private final Pattern<T2> second;

    private OrPattern(Pattern<T1> first, Pattern<T2> second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public <U, V> Pattern<LogicalPlan> with(Function<? super LogicalPlan, U> getProperty, Pattern<V> propertyPattern) {
        return new OrPattern<>(first, second.with(getProperty, propertyPattern));
    }

    @Override
    public Pattern<LogicalPlan> with(Predicate<? super LogicalPlan> propertyPredicate) {
        return new OrPattern<>(first, second.with(propertyPredicate));
    }

    @Override
    public Match<LogicalPlan> accept(Object object, Captures captures, UnaryOperator<LogicalPlan> resolvePlan) {
        Match<?> match = first.accept(object, captures, resolvePlan);
        if (!match.isPresent()) {
            match = second.accept(object, captures, resolvePlan);
        }
        return match.map(LogicalPlan.class::cast);
    }

    public static final class Builder<T1> {

        private final Pattern<T1> first;

        Builder(Pattern<T1> first) {
            this.first = first;
        }

        public <T2 extends LogicalPlan> Pattern<LogicalPlan> typeOf(Class<T2> expectedClass) {
            return new OrPattern<>(first, Pattern.typeOf(expectedClass));
        }
    }
}
