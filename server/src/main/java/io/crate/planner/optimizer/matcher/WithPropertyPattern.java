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

import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import io.crate.planner.operators.LogicalPlan;

public class WithPropertyPattern<T> extends Pattern<T> {

    private final Pattern<T> pattern;
    private final Predicate<? super T> propertyPredicate;

    WithPropertyPattern(Pattern<T> pattern, Predicate<? super T> propertyPredicate) {
        this.pattern = pattern;
        this.propertyPredicate = propertyPredicate;
    }

    @Override
    public Match<T> accept(Object object, Captures captures, UnaryOperator<LogicalPlan> resolvePlan) {
        Match<T> match = pattern.accept(object, captures, resolvePlan);
        return match.flatMap(matchedValue -> {
            if (propertyPredicate.test(matchedValue)) {
                return match;
            } else {
                return Match.empty();
            }
        });
    }
}
