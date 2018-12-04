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

import com.google.common.annotations.VisibleForTesting;
import io.crate.data.Input;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import javax.annotation.Nonnull;
import java.util.List;

public class SubstrFunction extends Scalar<String, Object> {

    public static final String NAME = "substr";

    private FunctionInfo info;

    private SubstrFunction(FunctionInfo info) {
        this.info = info;
    }

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new Resolver());
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public String evaluate(TransactionContext txnCtx, Input[] args) {
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

    private static String evaluate(@Nonnull String inputStr, int beginIdx) {
        final int startPos = Math.max(0, beginIdx - 1);
        if (startPos > inputStr.length() - 1) {
            return "";
        }
        int endPos = inputStr.length();
        return substring(inputStr, startPos, endPos);
    }

    @VisibleForTesting
    static String evaluate(@Nonnull String inputStr, int beginIdx, int len) {
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

    private static class Resolver extends BaseFunctionResolver {

        private static final Param INTEGER_COMPATIBLE_TYPES = Param.of(DataTypes.BYTE, DataTypes.SHORT, DataTypes.INTEGER);

        protected Resolver() {
            super(FuncParams.builder(
                Param.STRING,
                INTEGER_COMPATIBLE_TYPES)
                .withVarArgs(INTEGER_COMPATIBLE_TYPES).limitVarArgOccurrences(1)
                .build());
        }

        private static FunctionInfo createInfo(List<DataType> types) {
            return new FunctionInfo(new FunctionIdent(NAME, types), DataTypes.STRING);
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            return new SubstrFunction(createInfo(dataTypes));
        }
    }
}
