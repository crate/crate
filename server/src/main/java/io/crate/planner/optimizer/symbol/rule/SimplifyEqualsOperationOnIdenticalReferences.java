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

package io.crate.planner.optimizer.symbol.rule;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;

import java.util.List;

import io.crate.expression.operator.EqOperator;
import io.crate.expression.predicate.IsNullPredicate;
import io.crate.expression.predicate.NotPredicate;
import io.crate.expression.scalar.Ignore3vlFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.planner.optimizer.symbol.FunctionLookup;
import io.crate.planner.optimizer.symbol.Rule;

public class SimplifyEqualsOperationOnIdenticalReferences implements Rule<Function> {

    private final Pattern<Function> pattern;

    public SimplifyEqualsOperationOnIdenticalReferences() {
        this.pattern = typeOf(Function.class)
            .with(f -> EqOperator.NAME.equals(f.name()))
            .with(f -> f.arguments(), typeOf(List.class)
                .with(list -> list.get(0) instanceof Reference left &&
                              list.get(1) instanceof Reference right &&
                              left.equals(right))
            );
    }

    @Override
    public Pattern<Function> pattern() {
        return pattern;
    }

    @Override
    public Symbol apply(Function operator, Captures captures, NodeContext nodeCtx, FunctionLookup functionLookup, Symbol parentNode) {
        Reference ref = (Reference) operator.arguments().get(0);
        // if ref is not null or the parent node is ignore3vl
        if (ref.isNullable() == false || (parentNode != null && Ignore3vlFunction.NAME.equals(((Function) parentNode).name()))) {
            // WHERE COL = COL  =>  WHERE TRUE
            return Literal.BOOLEAN_TRUE;
        }
        // if ref is nullable it is 3vl, but having no parent node makes it implicitly 2vl
        if (ref.isNullable() && parentNode == null) {
            // WHERE COL = COL  =>  WHERE COL IS NOT NULL
            return functionLookup.get(
                NotPredicate.NAME,
                List.of(functionLookup.get(IsNullPredicate.NAME, List.of(ref)))
            );
        }
        return null;
    }
}
