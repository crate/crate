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

package io.crate.expression.scalar.string;

import java.util.Locale;
import java.util.function.BiFunction;

import io.crate.data.Input;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

public class StringLeftRightFunction extends Scalar<String, Object> {

    public static void register(Functions.Builder module) {
        module.add(
            Signature.scalar(
                "left",
                Feature.CONDITIONAL,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ).withFeature(Feature.DETERMINISTIC),
            (signature, boundSignature) ->
                new StringLeftRightFunction(signature, boundSignature, StringLeftRightFunction::left)
        );
        module.add(
            Signature.scalar(
                "right",
                Feature.CONDITIONAL,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ).withFeature(Feature.DETERMINISTIC),
            (signature, boundSignature) ->
                new StringLeftRightFunction(signature, boundSignature, StringLeftRightFunction::right)
        );
    }

    private final BiFunction<String, Integer, String> func;

    private StringLeftRightFunction(Signature signature,
                                    BoundSignature boundSignature,
                                    BiFunction<String, Integer, String> func) {
        super(signature, boundSignature);
        this.func = func;
    }

    @Override
    public String evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
        assert args.length == 2 : String.format(Locale.ENGLISH,
                                                "number of arguments must be 2, got %d instead",
                                                args.length);
        String str = (String) args[0].value();
        Number len = (Number) args[1].value();
        if (str == null || len == null) {
            return null;
        }
        return len.intValue() == 0 || str.isEmpty() ? "" : func.apply(str, len.intValue());
    }

    private static String left(String str, int len) {
        if (len > 0) {
            return str.substring(0, Math.min(len, str.length()));
        }
        final int finalLen = str.length() + len;
        return finalLen > 0 ? str.substring(0, finalLen) : "";
    }

    private static String right(String str, int len) {
        if (len < 0) {
            return str.substring(Math.min(-len, str.length()));
        }
        final int finalLen = str.length() - len;
        return finalLen <= 0 ? str : str.substring(finalLen);
    }
}
