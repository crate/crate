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

package io.crate.expression.scalar.timestamp;

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.List;

public final class NowFunction extends Scalar<Long, Object> {

    public static final String NAME = "now";
    private static final FunctionInfo INFO = new FunctionInfo(
        new FunctionIdent(null, NAME, List.of()),
        DataTypes.TIMESTAMPZ,
        FunctionInfo.Type.SCALAR
    );

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.TIMESTAMPZ.getTypeSignature()
            ),
            (signature, args) -> new NowFunction(signature)
        );
    }

    private final Signature signature;

    public NowFunction(Signature signature) {
        this.signature = signature;
    }

    @Override
    @SafeVarargs
    public final Long evaluate(TransactionContext txnCtx, Input<Object>... args) {
        return txnCtx.currentTimeMillis();
    }

    @Override
    public FunctionInfo info() {
        return INFO;
    }

    @Nullable
    @Override
    public Signature signature() {
        return signature;
    }
}
