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
import io.crate.expression.scalar.ThreeParametersFunction;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.Locale;

public class StringPaddingFunction extends Scalar<String, Object> {

    public static final String LNAME = "lpad";
    public static final String RNAME = "rpad";
    public static final String DEFAULT_FILL = " ";
    public static final int LEN_LIMIT = 50000;

    public static void register(ScalarFunctionModule module) {
        // lpad(string1, len)
        module.register(
            Signature.scalar(
                LNAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            (signature, argumentTypes) ->
                new StringPaddingFunction(
                    new FunctionInfo(new FunctionIdent(LNAME, argumentTypes), DataTypes.STRING),
                    signature,
                    StringPaddingFunction::lpad
                )
        );
        // lpad(string1, len, string2)
        module.register(
            Signature.scalar(
                LNAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            (signature, argumentTypes) ->
                new StringPaddingFunction(
                    new FunctionInfo(new FunctionIdent(LNAME, argumentTypes), DataTypes.STRING),
                    signature,
                    StringPaddingFunction::lpad
                )
        );
        // rpad(string1, len)
        module.register(
            Signature.scalar(
                RNAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            (signature, argumentTypes) ->
                new StringPaddingFunction(
                    new FunctionInfo(new FunctionIdent(RNAME, argumentTypes), DataTypes.STRING),
                    signature,
                    StringPaddingFunction::rpad
                )
        );
        // rpad(string1, len, string2)
        module.register(
            Signature.scalar(
                RNAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            (signature, argumentTypes) ->
                new StringPaddingFunction(
                    new FunctionInfo(new FunctionIdent(RNAME, argumentTypes), DataTypes.STRING),
                    signature,
                    StringPaddingFunction::rpad
                )
        );
    }

    private final FunctionInfo info;
    private final Signature signature;
    private final ThreeParametersFunction<char[], Integer, char[], String> func;

    private StringPaddingFunction(FunctionInfo info,
                                  Signature signature,
                                  ThreeParametersFunction<char[], Integer, char[], String> func) {
        this.info = info;
        this.signature = signature;
        this.func = func;
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
    public String evaluate(TransactionContext txnCtx, Input[] args) {
        assert args.length == 2 || args.length == 3 : String.format(
            Locale.ENGLISH,
            "number of arguments must be 2 (optionally 3), got %d instead",
            args.length);

        String str = (String) args[0].value();
        Number len = (Number) args[1].value();
        String fill = args.length == 3 ? (String) args[2].value() : DEFAULT_FILL;
        if (str == null || len == null || fill == null) {
            return null;
        }
        int lenValue = len.intValue();
        if (lenValue > LEN_LIMIT) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                             "len argument exceeds predefined limit of %d",
                                                             LEN_LIMIT));
        }

        if (lenValue <= 0 || (str.isEmpty() && fill.isEmpty())) {
            return "";
        } else if (str.length() >= lenValue) {
            return str.substring(0, lenValue);
        } else if (fill.isEmpty()) {
            return str;
        }
        return func.apply(str.toCharArray(), lenValue, fill.toCharArray());
    }

    private static String lpad(char[] srcChars, int len, char[] fillChars) {
        char[] buffer = new char[len];
        int padLen = len - srcChars.length;
        System.arraycopy(srcChars, 0, buffer, padLen, srcChars.length);
        for (int i = 0; i < padLen; i++) {
            buffer[i] = fillChars[i % fillChars.length];
        }
        return String.valueOf(buffer);
    }

    private static String rpad(char[] srcChars, int len, char[] fillChars) {
        char[] buffer = new char[len];
        System.arraycopy(srcChars, 0, buffer, 0, srcChars.length);
        for (int i = srcChars.length, j = 0; i < len; i++, j++) {
            buffer[i] = fillChars[j % fillChars.length];
        }
        return String.valueOf(buffer);
    }
}
