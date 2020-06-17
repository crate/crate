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

package io.crate.expression.scalar.string;

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

import java.util.List;

public final class StringRepeatFunction extends Scalar<String, Object> {

    private static final FunctionInfo INFO = new FunctionInfo(
        new FunctionIdent(
            "repeat",
            List.of(DataTypes.STRING, DataTypes.INTEGER)
        ),
        DataTypes.STRING
    );

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                "repeat",
                DataTypes.STRING.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            (signature, argumentTypes) -> new StringRepeatFunction(signature)
        );
    }

    private final Signature signature;

    public StringRepeatFunction(Signature signature) {
        this.signature = signature;
    }

    @Override
    public String evaluate(TransactionContext txnCtx, Input<Object>[] args) {
        assert args.length == 2 : "repeat takes exactly two arguments";
        var text = (String) args[0].value();
        var repetitions = (Integer) args[1].value();
        if (text == null || repetitions == null) {
            return null;
        }

        if (repetitions <= 0) {
            return "";
        } else {
            return text.repeat(repetitions);
        }
    }

    @Override
    public FunctionInfo info() {
        return INFO;
    }

    @Override
    public Signature signature() {
        return signature;
    }
}
