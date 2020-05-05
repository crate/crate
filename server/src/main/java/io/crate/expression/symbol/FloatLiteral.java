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

package io.crate.expression.symbol;

import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.math.BigDecimal;

/**
 * This is a literal for handling explicit and implicit casts of <p>float</p> values.
 * It will be streamed as a <p>Literal<Float>()</p> and thus special casting is only available during the
 * analyzer and plan phases.
 */
public class FloatLiteral extends Literal<Float> {

    private final BigDecimal bd;

    public FloatLiteral(BigDecimal bd) {
        super(DataTypes.FLOAT, bd.floatValue());
        this.bd = bd;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return super.accept(visitor, context);
    }

    @Override
    public Symbol cast(DataType<?> targetType, boolean tryCast, boolean explicitCast) {
        // To prevent floating number rounding issues, casting it to Double will use
        // the double of the parsed BigDecimal value.
        if (targetType.id() == DataTypes.DOUBLE.id()) {
            return new Literal<>(DataTypes.DOUBLE, bd.doubleValue());
        }

        // On explicit casts, any further implicit cast is based on the float value.
        if (explicitCast && targetType.id() == DataTypes.FLOAT.id()) {
            return new Literal<>(DataTypes.FLOAT, bd.floatValue());
        }

        return super.cast(targetType, tryCast, explicitCast);
    }

    public Literal<Float> negate() {
        // BigDecimal does not support negating of ZERO values like -0.0, it always return 0.0.
        // Thus we return a generic float literal here as no BigDecimal information(original double value)
        // is needed later on (casts) for zero values.
        if (bd.intValue() == 0) {
            return Literal.of(bd.floatValue() * -1);
        }
        return new FloatLiteral(bd.negate());
    }
}
