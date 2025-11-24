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

package io.crate.expression.scalar.cast;

import io.crate.data.Input;
import io.crate.exceptions.ConversionException;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.TypeVariableConstraint;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;

public class ImplicitCastFunction extends Scalar<Object, Object> {

    public static final String NAME = "_cast";
    public static final Signature SIGNATURE = Signature.builder(NAME, FunctionType.SCALAR)
        .argumentTypes(TypeSignature.E)
        // concrete returnType is part of the `Function` symbol and will be available in the BoundSignature.
        .returnType(DataTypes.UNDEFINED.getTypeSignature())
        .features(Feature.DETERMINISTIC)
        .typeVariableConstraints(TypeVariableConstraint.E)
        .build();

    static final Signature BWC_SIGNATURE = Signature.builder(NAME, FunctionType.SCALAR)
        .argumentTypes(TypeSignature.E, DataTypes.STRING.getTypeSignature())
        .returnType(DataTypes.UNDEFINED.getTypeSignature())
        .features(Feature.DETERMINISTIC)
        .typeVariableConstraints(TypeVariableConstraint.E)
        .build();


    public static void register(Functions.Builder module) {
        module.add(SIGNATURE, ImplicitCastFunction::new);

        // BWC signature for:
        //
        // - Mixed clusters (handler created old function symbol)
        // - Tables created in 6.0 or 6.1 where the cluster state can contain old cast functions
        //   like: create table tbl (x int, y long generated always as x + 1)
        //
        // Relying on `boundSignature.returnType()` for the cast implementation
        // itself still works because the boundSignature always resolves to a
        // more concrete type than `undefined`.
        module.add(BWC_SIGNATURE, ImplicitCastFunction::new);
    }

    private ImplicitCastFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    public Object evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>[] args) {
        assert args.length == 1 || args.length == 2 : "number of args must be 1 or 2";
        var arg = args[0].value();
        return cast(arg, boundSignature.returnType());
    }

    private static Object cast(Object value, DataType<?> type) {
        try {
            return type.implicitCast(value);
        } catch (ConversionException e) {
            throw e;
        } catch (ClassCastException | IllegalArgumentException e) {
            throw new ConversionException(value, type);
        }
    }

    @Override
    public Symbol normalizeSymbol(io.crate.expression.symbol.Function symbol,
                                  TransactionContext txnCtx,
                                  NodeContext nodeCtx) {
        Symbol argument = symbol.arguments().get(0);
        DataType<?> targetType = boundSignature.returnType();

        if (argument.valueType().equals(targetType)) {
            return argument;
        }
        if (argument instanceof Input<?> input) {
            Object value = input.value();
            try {
                return Literal.ofUnchecked(targetType, targetType.implicitCast(value));
            } catch (ConversionException e) {
                throw e;
            } catch (ClassCastException | IllegalArgumentException e) {
                throw new ConversionException(argument, targetType);
            }
        }
        return symbol;
    }
}
