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

package io.crate.expression.scalar;

import io.crate.data.Input;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Scalar function to resolve elements inside a map.
 */
public class SubscriptObjectFunction extends Scalar<Object, Map<String, Object>> {

    public static final String NAME = "subscript_obj";

    public static final Signature SIGNATURE = Signature
        .scalar(
            NAME,
            DataTypes.UNTYPED_OBJECT.getTypeSignature(),
            DataTypes.STRING.getTypeSignature(),
            DataTypes.UNDEFINED.getTypeSignature())
        .withVariableArity();

    public static void register(ScalarFunctionModule module) {
        module.register(
            SIGNATURE,
            SubscriptObjectFunction::new
        );
    }

    private final Signature signature;
    private final Signature boundSignature;

    private SubscriptObjectFunction(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
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
    public Symbol normalizeSymbol(Function func, TransactionContext txnCtx, NodeContext nodeCtx) {
        Symbol result = evaluateIfLiterals(this, txnCtx, nodeCtx, func);
        if (result instanceof Literal) {
            return result;
        }
        return tryToInferReturnTypeFromObjectTypeAndArguments(func);
    }

    static Symbol tryToInferReturnTypeFromObjectTypeAndArguments(Function func) {
        if (!func.valueType().equals(DataTypes.UNDEFINED)) {
            return func;
        }
        var arguments = func.arguments();
        ObjectType objectType = (ObjectType) arguments.get(0).valueType();
        List<String> path = maybeCreatePath(arguments);
        if (path == null) {
            return func;
        } else {
            DataType<?> returnType = objectType.resolveInnerType(path);
            return returnType.equals(DataTypes.UNDEFINED)
                ? func
                : new Function(
                    func.signature(),
                    func.arguments(),
                    returnType
            );
        }
    }

    @Nullable
    private static List<String> maybeCreatePath(List<Symbol> arguments) {
        List<String> path = null;
        for (int i = 1; i < arguments.size(); i++) {
            Symbol arg = arguments.get(i);
            if (arg instanceof Literal) {
                if (path == null) {
                    path = new ArrayList<>();
                }
                path.add(DataTypes.STRING.sanitizeValue(((Literal<?>) arg).value()));
            } else {
                return null;
            }
        }
        return path;
    }

    @Override
    @SafeVarargs
    public final Object evaluate(TransactionContext txnCtx, NodeContext ndeCtx, Input<Map<String, Object>>... args) {
        assert args.length >= 2 : NAME + " takes 2 or more arguments, got " + args.length;
        Object mapValue = args[0].value();
        for (var i = 1; i < args.length; i++) {
            if (mapValue == null) {
                return null;
            }
            mapValue = SubscriptFunction.lookupByName(mapValue, args[i].value(), txnCtx.sessionSettings().errorOnUnknownObjectKey());
        }
        return mapValue;
    }
}
