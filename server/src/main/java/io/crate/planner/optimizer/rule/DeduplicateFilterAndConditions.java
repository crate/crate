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

import java.util.List;
import java.util.LinkedHashSet;
import java.util.Set;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;

import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.FunctionCopyVisitor;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.operators.Filter;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

public class DeduplicateFilterAndConditions implements Rule<Filter> {

    private final Pattern<Filter> pattern = typeOf(Filter.class);

    private static final Visitor VISITOR = new Visitor();

    @Override
    public Pattern<Filter> pattern() {
        return pattern;
    }

    @Override
    public Filter apply(Filter plan, Captures captures, Rule.Context context) {
        Symbol query = plan.query();
        Symbol newQuery = query.accept(VISITOR, null);
        if (newQuery != query) {
            return new Filter(plan.source(), newQuery);
        }
        return null;
    }

    static class Visitor extends FunctionCopyVisitor<Void> {

        @Override
        public Symbol visitFunction(Function func, Void context) {
            if (func.name().equals(AndOperator.NAME)) {
                Symbol newFunc = super.visitFunction(func, context);
                List<Symbol> conjunctions = AndOperator.split(newFunc);
                Set<Symbol> uniqueConjunctions = new LinkedHashSet<>(conjunctions);
                if (uniqueConjunctions.size() < conjunctions.size()) {
                    return AndOperator.join(uniqueConjunctions);
                }
                return newFunc;
            }
            return super.visitFunction(func, context);
        }

    }

}
