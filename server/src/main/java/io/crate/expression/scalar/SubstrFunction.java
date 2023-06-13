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

import java.util.List;

import org.jetbrains.annotations.NotNull;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.data.Input;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

public class SubstrFunction extends Scalar<String, Object> {

    public static final String NAME = "substr";
    public static final String ALIAS = "substring";

    public static void register(ScalarFunctionModule module) {
        for (var name : List.of(NAME, ALIAS)) {
            module.register(
                Signature.scalar(
                    name,
                    DataTypes.STRING.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature()
                ),
                SubstrFunction::new
            );
            module.register(
                Signature.scalar(
                    name,
                    DataTypes.STRING.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature()
                ),
                SubstrFunction::new
            );
        }
    }

    private final Signature signature;
    private final BoundSignature boundSignature;

    private SubstrFunction(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public BoundSignature boundSignature() {
        return boundSignature;
    }

    @Override
    public String evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
        assert args.length == 2 || args.length == 3 : "number of arguments must be 2 or 3";
        String val = (String) args[0].value();
        if (val == null) {
            return null;
        }
        Number beginIdx = (Number) args[1].value();
        if (beginIdx == null) {
            return null;
        }
        if (args.length == 3) {
            Number len = (Number) args[2].value();
            if (len == null) {
                return null;
            }
            return evaluate(val, (beginIdx).intValue(), len.intValue());

        }
        return evaluate(val, (beginIdx).intValue());
    }

    private static String evaluate(@NotNull String inputStr, int beginIdx) {
        final int startPos = Math.max(0, beginIdx - 1);
        if (startPos > inputStr.length() - 1) {
            return "";
        }
        int endPos = inputStr.length();
        return substring(inputStr, startPos, endPos);
    }

    @VisibleForTesting
    static String evaluate(@NotNull String inputStr, int beginIdx, int len) {
        final int startPos = Math.max(0, beginIdx - 1);
        if (startPos > inputStr.length() - 1) {
            return "";
        }
        int endPos = inputStr.length();
        if (startPos + len < endPos) {
            endPos = startPos + len;
        }
        return substring(inputStr, startPos, endPos);
    }

    @VisibleForTesting
    static String substring(String value, int begin, int end) {
        return value.substring(begin, end);
    }
}
