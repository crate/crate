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
import java.util.function.UnaryOperator;

import io.crate.planner.operators.LogicalPlan;

class WithPattern<T, U, V> extends Pattern<T> {

    private final Pattern<T> firstPattern;
    private final Function<? super T, U> getProperty;
    private final Pattern<V> propertyPattern;

    WithPattern(Pattern<T> firstPattern, Function<? super T, U> getProperty, Pattern<V> propertyPattern) {
        this.firstPattern = firstPattern;
        this.getProperty = getProperty;
        this.propertyPattern = propertyPattern;
    }

    @Override
    public Match<T> accept(Object object, Captures captures, UnaryOperator<LogicalPlan> resolvePlan) {
        Match<T> match = firstPattern.accept(object, captures, resolvePlan);
        return match.flatMap(matchedValue -> {
            Object value = getProperty.apply(matchedValue);
            if (value == null) {
                return match;
            }
            value = value instanceof LogicalPlan logicalPlan ? resolvePlan.apply(logicalPlan) : value;
            return propertyPattern.accept(value, match.captures(), resolvePlan)
                .map(ignored -> match.value());
        });
    }
}
