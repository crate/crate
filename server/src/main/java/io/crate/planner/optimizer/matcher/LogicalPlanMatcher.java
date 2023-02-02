/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.Optional;
import java.util.function.Function;

import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.memo.GroupReference;
import io.crate.planner.optimizer.memo.GroupReferenceResolver;

public class LogicalPlanMatcher extends DefaultMatcher {

    private final GroupReferenceResolver groupReferenceResolver;

    public LogicalPlanMatcher(GroupReferenceResolver groupReferenceResolver) {
        this.groupReferenceResolver = groupReferenceResolver;
    }

    @Override
    public <T, U, V> Match<T> matchWith(WithPattern<T, U, V> withPattern, Object object, Captures captures) {
        Object resolvedObject = object;
        if (object instanceof GroupReference) {
            resolvedObject = groupReferenceResolver.apply((LogicalPlan) object);
        }
        Match<T> match = withPattern.firstPattern().accept(this, resolvedObject, captures);
        Function<? super T, Optional<U>> property = withPattern.getProperty();
        Optional<?> propertyValue = property.apply((T) object);

        Optional<?> resolvedValue = propertyValue
            .map(value -> value instanceof GroupReference ? groupReferenceResolver.apply(((GroupReference) value)) : value);

        Match<?> propertyMatch = resolvedValue
            .map(value -> match(withPattern.propertyPattern(), value, captures))
            .orElse(Match.empty());
        return propertyMatch.map(ignored -> (T) object);
    }
}
