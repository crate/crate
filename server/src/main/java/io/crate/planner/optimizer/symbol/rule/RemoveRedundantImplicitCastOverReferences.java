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

package io.crate.planner.optimizer.symbol.rule;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;

import org.jspecify.annotations.Nullable;

import io.crate.expression.scalar.cast.ImplicitCastFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.planner.optimizer.symbol.FunctionLookup;
import io.crate.planner.optimizer.symbol.Rule;
import io.crate.types.ArrayType;
import io.crate.types.DataType;

/**
 * Sometimes redundant casts are introduced, i.e.:: for numeric_array column whose type is `NUMERIC(p,s)`, queries like
 * `numeric_array = [1.11, 1.12]::numeric[]` resolve to `op_=(NUMERIC(null, null), NUMERIC(null,null))` such that
 * numeric_array is cast to NUMERIC(null, null) by {@link io.crate.analyze.expressions.ExpressionAnalyzer#cast}. However,
 * it is an implicit cast(to the same type) that the values are not intended to be modified in any way, the cast is
 * redundant and can be removed.
 */
public class RemoveRedundantImplicitCastOverReferences implements Rule<Function> {

    private final Pattern<Function> pattern;

    public RemoveRedundantImplicitCastOverReferences() {
        this.pattern = typeOf(Function.class)
            .with(fn -> isCastOnReference(fn));
    }

    private static boolean isCastOnReference(Function fn) {
        if (!fn.name().equals(ImplicitCastFunction.NAME)) {
            return false;
        }
        Symbol arg = fn.arguments().getFirst();
        DataType<?> targetType = fn.valueType();
        DataType<?> sourceType = arg.valueType();
        return arg instanceof Reference
            && ArrayType.unnest(targetType).id() == ArrayType.unnest(sourceType).id()
            && ArrayType.dimensions(targetType) == ArrayType.dimensions(sourceType);
    }

    @Override
    public Pattern<Function> pattern() {
        return pattern;
    }

    @Override
    public Symbol apply(Function cast,
                        Captures captures,
                        NodeContext nodeCtx,
                        FunctionLookup functionLookup,
                        @Nullable Symbol parentNode) {
        return cast.arguments().getFirst();
    }
}
