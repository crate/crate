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

import io.crate.expression.operator.any.AnyEqOperator;
import io.crate.expression.operator.any.AnyNeqOperator;
import io.crate.expression.operator.any.AnyOperator;
import io.crate.expression.scalar.cast.CastMode;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.metadata.NodeContext;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.planner.optimizer.symbol.FunctionLookup;
import io.crate.planner.optimizer.symbol.Rule;
import io.crate.types.ArrayType;
import io.crate.types.DataType;


public class MoveReferenceCastToLiteralCastOnAnyOperatorsWhenRightIsReference implements Rule<Function> {

    private final Capture<Function> castCapture;
    private final Pattern<Function> pattern;

    public MoveReferenceCastToLiteralCastOnAnyOperatorsWhenRightIsReference() {
        this.castCapture = new Capture<>();
        this.pattern = typeOf(Function.class)
            .with(f -> AnyOperator.OPERATOR_NAMES.contains(f.name()))
            .with(f -> f.arguments().get(0).symbolType().isValueOrParameterSymbol())
            .with(f -> f.arguments().get(1), typeOf(Function.class).capturedAs(castCapture)
                .with(f -> f.isCast() && CastMode.IMPLICIT.equals(f.castMode()))
                .with(f -> f.arguments().get(0).symbolType() == SymbolType.REFERENCE)
            );
    }

    @Override
    public Pattern<Function> pattern() {
        return pattern;
    }

    @Override
    public Symbol apply(Function operator,
                        Captures captures,
                        NodeContext nodeCtx,
                        FunctionLookup functionLookup,
                        Symbol parentNode) {
        var literalOrParam = operator.arguments().get(0);
        var castFunction = captures.get(castCapture);
        var reference = castFunction.arguments().get(0);
        DataType<?> targetType = reference.valueType();
        if (targetType.id() != ArrayType.ID) {
            return null;
        }
        targetType = ((ArrayType<?>) targetType).innerType();

        var operatorName = operator.name();
        if (List.of(AnyEqOperator.NAME, AnyNeqOperator.NAME).contains(operatorName) == false
            && literalOrParam.valueType().id() == ArrayType.ID) {
            // this is not supported and will fail later on with more verbose error than a cast error
            return null;
        }

        return functionLookup.get(
            operator.name(),
            List.of(literalOrParam.cast(targetType), reference)
        );
    }
}
