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
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;

public class ImplicitCastFunction extends Scalar<Object, Object> {

    public static final String NAME = "_cast";

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                NAME,
                parseTypeSignature("E"),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.UNDEFINED.getTypeSignature()
            ).withTypeVariableConstraints(typeVariable("E")),
            ImplicitCastFunction::new
        );
    }

    private final Signature signature;
    private final Signature boundSignature;

    private ImplicitCastFunction(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public Object evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>[] args) {
        var targetTypeSignature = parseTypeSignature((String) args[1].value());
        var targetType = targetTypeSignature.createType();
        try {
            return targetType.implicitCast(args[0].value());
        } catch (ConversionException e) {
            throw e;
        } catch (ClassCastException | IllegalArgumentException e) {
            throw new ConversionException(args[0].value(), targetType);
        }
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    @Override
    public Symbol normalizeSymbol(io.crate.expression.symbol.Function symbol,
                                  TransactionContext txnCtx,
                                  NodeContext nodeCtx) {
        Symbol argument = symbol.arguments().get(0);

        var targetTypeAsString = (String) ((Input<?>) symbol.arguments().get(1)).value();
        var targetType = parseTypeSignature(targetTypeAsString).createType();

        if (argument.valueType().equals(targetType)) {
            return argument;
        }

        if (argument instanceof Input) {
            Object value = ((Input<?>) argument).value();
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
