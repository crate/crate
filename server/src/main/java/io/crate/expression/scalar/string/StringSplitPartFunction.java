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

import io.crate.data.Input;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

/**
 * String split_part(String text, String delimiter, Integer index)
 * <p>
 * Splits a text on delimiter and returns the and returns the part at index (1 for the first part)
 */
public final class StringSplitPartFunction extends Scalar<String, Object> {

    public static void register(Functions.Builder module) {
        module.add(
            Signature.builder("split_part", FunctionType.SCALAR)
                .argumentTypes(DataTypes.STRING.getTypeSignature(), DataTypes.STRING.getTypeSignature(), DataTypes.INTEGER.getTypeSignature())
                .returnType(DataTypes.STRING.getTypeSignature())
                .features(Feature.DETERMINISTIC)
                .build(),
            StringSplitPartFunction::new
        );
    }

    public StringSplitPartFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    public String evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>[] args) {
        assert args.length == 3 : "split_part takes exactly three arguments";
        var text = (String) args[0].value();
        var delimiter = (String) args[1].value();
        var indexB = (Integer) args[2].value();
        if (text == null || delimiter == null || indexB == null) {
            return null;
        }
        int index = indexB;
        if (index < 1) {
            throw new IllegalArgumentException("index in split_part must be greater than zero");
        }

        // special case for empty delimiter
        if (delimiter.isEmpty()) {
            if (index == 1) {
                return text;
            } else {
                return "";
            }
        }

        int startIndex = 0;
        for (int i = 1; i < index; i++) {
            int pos = text.indexOf(delimiter, startIndex);
            if (pos < 0) {
                return "";
            }
            startIndex = pos + delimiter.length();
        }
        int endIndex = text.indexOf(delimiter, startIndex);
        if (endIndex < 0) {
            endIndex = text.length();
        }
        return text.substring(startIndex, endIndex);
    }
}
