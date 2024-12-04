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

import static io.crate.expression.operator.Operators.COMPARISON_OPERATORS;
import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static io.crate.types.DataTypes.isSafeConversion;

import java.math.BigDecimal;
import java.util.List;

import io.crate.expression.scalar.cast.CastMode;
import io.crate.expression.scalar.cast.ImplicitCastFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.planner.optimizer.symbol.FunctionLookup;
import io.crate.planner.optimizer.symbol.Rule;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class SwapCastsInComparisonOperators implements Rule<Function> {

    private final Capture<Function> castCapture;
    private final Pattern<Function> pattern;

    /**
     * A cast is 'swappable' if the cast does not cause loss of precision. This method executes the casts in uncertain
     * situations. i.e.:: casting 126 type of int to a byte, which is a narrowing conversion, is convertable since 126
     * does fit in a byte.
     */
    private static boolean isSwappable(Function cmpOp) {
        if (cmpOp.arguments().get(1) instanceof Literal<?> literal) {
            Function cast = (Function) cmpOp.arguments().getFirst();
            Reference ref = (Reference) cast.arguments().getFirst();
            DataType<?> refInnerType = ArrayType.unnest(ref.valueType());
            DataType<?> literalInnerType = ArrayType.unnest(literal.valueType());
            if (!DataTypes.isNumeric(literalInnerType) || !DataTypes.isNumeric(refInnerType)) {
                return true;
            }
            if (isSafeConversion(literalInnerType, refInnerType)) {
                return true;
            }
            return isNarrowingConversionPossible(literal.value(), ref.valueType());
        }
        return true;
    }

    /**
     * Executes a narrowing conversion to verify that it is possible.
     */
    private static boolean isNarrowingConversionPossible(Object value, DataType<?> targetType) {
        if (DataTypes.isArray(targetType)) {
            return ((List<?>) value).stream().allMatch(e -> isNarrowingConversionPossible(e, ((ArrayType<?>) targetType).innerType()));
        } else {
            try {
                Number n = (Number) value;
                var literalStr = String.valueOf(n);
                var castedStr = String.valueOf(targetType.implicitCast(n));
                return new BigDecimal(literalStr).compareTo(new BigDecimal(castedStr)) == 0;
            } catch (IllegalArgumentException | ClassCastException e) {
                return false;
            }
        }
    }

    public SwapCastsInComparisonOperators() {
        this.castCapture = new Capture<>();
        this.pattern = typeOf(Function.class)
            .with(f -> COMPARISON_OPERATORS.contains(f.name()))
            .with(f -> f.arguments().get(1).symbolType().isValueOrParameterSymbol())
            .with(f -> f.arguments().get(0), typeOf(Function.class).capturedAs(castCapture)
                // We have to respect explicit casts, see https://github.com/crate/crate/issues/12135
                .with(f -> f.name().equals(ImplicitCastFunction.NAME))
                .with(f -> f.arguments().get(0).symbolType() == SymbolType.REFERENCE)
            ).with(SwapCastsInComparisonOperators::isSwappable);
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
        var literalOrParam = operator.arguments().get(1);
        var castFunction = captures.get(castCapture);
        var reference = castFunction.arguments().get(0);
        DataType<?> targetType = reference.valueType();
        CastMode castMode = castFunction.castMode();
        assert castMode != null : "Pattern matched, function must be a cast";
        Symbol castedLiteral = literalOrParam.cast(targetType, castMode);
        // Can't use functionResolver here because it would attempt to re-resolve the function
        // and re-add casts if there is a text/varchar(n) missmatch.
        return new Function(operator.signature(), List.of(reference, castedLiteral), operator.valueType());
    }
}
