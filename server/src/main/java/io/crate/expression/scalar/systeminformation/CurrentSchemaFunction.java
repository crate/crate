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

package io.crate.expression.scalar.systeminformation;

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FunctionName;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;


public class CurrentSchemaFunction extends Scalar<String, Object> {

    public static final String NAME = "current_schema";

    private static final FunctionName FQN = new FunctionName(PgCatalogSchemaInfo.NAME, NAME);

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                FQN,
                DataTypes.STRING.getTypeSignature()
            ).withFeatures(NO_FEATURES),
            CurrentSchemaFunction::new
        );
    }

    private final Signature signature;
    private final Signature boundSignature;

    public CurrentSchemaFunction(Signature signature, Signature boundSignature) {
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
    public String evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>... args) {
        assert args.length == 0 : "number of args must be 0";
        return txnCtx.sessionSettings().currentSchema();
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, @Nullable TransactionContext txnCtx, NodeContext nodeCtx) {
        if (txnCtx == null) {
            return Literal.NULL;
        }
        return Literal.of(txnCtx.sessionSettings().currentSchema());
    }
}
