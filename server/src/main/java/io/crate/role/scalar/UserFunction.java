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

package io.crate.role.scalar;

import org.jetbrains.annotations.Nullable;

import io.crate.data.Input;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

public class UserFunction extends Scalar<String, Object> {

    public static final String CURRENT_USER_FUNCTION_NAME = "current_user";
    public static final String SESSION_USER_FUNCTION_NAME = "session_user";

    public static void register(Functions.Builder builder) {
        builder.add(
            Signature.scalar(
                CURRENT_USER_FUNCTION_NAME,
                DataTypes.STRING.getTypeSignature()
            ).withFeatures(Scalar.NO_FEATURES),
            UserFunction::new
        );
        builder.add(
            Signature.scalar(
                SESSION_USER_FUNCTION_NAME,
                DataTypes.STRING.getTypeSignature()
            ).withFeatures(Scalar.NO_FEATURES),
            UserFunction::new
        );
    }

    public UserFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    public String evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>... args) {
        assert args.length == 0 : "number of args must be 0";
        return txnCtx.sessionSettings().userName();
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, @Nullable TransactionContext txnCtx, NodeContext nodeCtx) {
        if (txnCtx == null) {
            return Literal.NULL;
        }
        return Literal.of(txnCtx.sessionSettings().userName());
    }
}
