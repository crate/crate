/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class StringToArrayFunction extends Scalar<List<String>, String> {

    private static final String NAME = "string_to_array";

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING_ARRAY.getTypeSignature()
            ),
            (signature, argumentTypes) ->
                new StringToArrayFunction(
                    new FunctionInfo(new FunctionIdent(NAME, argumentTypes), DataTypes.STRING_ARRAY),
                    signature
                )
        );
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING_ARRAY.getTypeSignature()
            ),
            (signature, argumentTypes) ->
                new StringToArrayFunction(
                    new FunctionInfo(new FunctionIdent(NAME, argumentTypes), DataTypes.STRING_ARRAY),
                    signature
                )
        );
    }

    private final FunctionInfo info;
    private final Signature signature;

    private StringToArrayFunction(FunctionInfo info, Signature signature) {
        this.info = info;
        this.signature = signature;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Nullable
    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public List<String> evaluate(TransactionContext txnCtx, Input<String>[] args) {
        assert args.length == 2 || args.length == 3 : "number of args must be 2 or 3";

        String str = args[0].value();
        if (str == null) {
            return null;
        }
        if (str.isEmpty()) {
            return List.of();
        }

        String separator = args[1].value();
        String nullStr = null;
        if (args.length == 3) {
            nullStr = args[2].value();
        }

        return split(str, separator, nullStr);
    }

    /**
     * <p>Splits a {@code String} into an array of strings using a separator
     * string specified and a null-string to set the substring to {@code null}
     * if they match.
     * </p>
     *
     * <p>
     * A {@code null} separator splits the string into array in which each
     * string characters becomes a separate element. If the separator is an
     * empty string, then the input string is returned as a one element array.
     * </p>
     *
     * @param str       The input {@code String} to split.
     * @param separator The separator {@code String} used for splitting
     *                  the input string into substrings. May be {@code null}.
     * @param nullStr   The null string used to set substrings to {@code null}
     *                  if they match. May be {@code null}.
     * @return An array of {@code String}.
     */
    private static List<String> split(@Nonnull String str, @Nullable String separator, @Nullable String nullStr) {

        if (separator == null) {
            ArrayList<String> subStrings = new ArrayList<>(str.length());
            for (int i = 0; i < str.length(); i++) {
                String subStr = String.valueOf(str.charAt(i));
                subStrings.add(setToNullIfMatch(subStr, nullStr));
            }
            return subStrings;
        } else if (separator.isEmpty()) {
            return Collections.singletonList(setToNullIfMatch(str, nullStr));
        } else {
            ArrayList<String> subStrings = new ArrayList<>();
            int start = 0;                      // search start position
            int pos = str.indexOf(separator);   // separator position

            while (pos >= start) {
                String subStr;

                if (pos > start) {
                    subStr = str.substring(start, pos);
                } else {
                    // consecutive occurrence of the separator,
                    // replace it with empty string
                    subStr = "";
                }

                start = pos + separator.length();
                pos = str.indexOf(separator, start);

                subStrings.add(setToNullIfMatch(subStr, nullStr));
            }
            String subStr = str.substring(start);
            subStrings.add(setToNullIfMatch(subStr, nullStr));
            return subStrings;
        }
    }

    @Nullable
    private static String setToNullIfMatch(String subStr, String nullStr) {
        if (Objects.equals(subStr, nullStr)) {
            return null;
        }
        return subStr;
    }
}
