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

package io.crate.planner.optimizer.symbol.rule;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.crate.expression.operator.LikeOperators;
import io.crate.expression.scalar.cast.CastMode;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.planner.optimizer.symbol.FunctionSymbolResolver;
import io.crate.planner.optimizer.symbol.Rule;
import io.crate.types.StringType;

public class SwapCastsInLikeOperators implements Rule<Function> {

    private final Set<String> LIKE_OPERATORS = Set.of(LikeOperators.OP_LIKE, LikeOperators.OP_ILIKE);

    private final Capture<Function> castCapture;
    private final Pattern<Function> pattern;

    public SwapCastsInLikeOperators(FunctionSymbolResolver functionResolver) {
        this.castCapture = new Capture<>();
        this.pattern = typeOf(Function.class)
            .with(f -> LIKE_OPERATORS.contains(f.name()))
            .with(f -> f.arguments().get(1).symbolType() == SymbolType.LITERAL)
            .with(f -> Optional.of(f.arguments().get(0)), typeOf(Function.class).capturedAs(castCapture)
                .with(f -> f.isCast())
                .with(f -> f.arguments().get(0) instanceof Reference ref && ref.valueType().id() == StringType.ID)
            );
    }

    @Override
    public Pattern<Function> pattern() {
        return pattern;
    }

    @Override
    public Symbol apply(Function likeFunction, Captures captures, NodeContext nodeCtx, Symbol parentNode) {
        var literal = likeFunction.arguments().get(1);
        var castFunction = captures.get(castCapture);
        var reference = castFunction.arguments().get(0);
        CastMode castMode = castFunction.castMode();
        assert castMode != null : "Pattern matched, function must be a cast";
        Symbol castedLiteral = literal.cast(StringType.INSTANCE, castMode);
        return new Function(likeFunction.signature(), List.of(reference, castedLiteral), likeFunction.valueType());
    }
}
