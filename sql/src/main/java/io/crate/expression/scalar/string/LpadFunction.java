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
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.types.DataType;
import io.crate.types.StringType;

import java.util.Collections;
import java.util.List;


public final class LpadFunction {

    private static final String NAME = "lpad";

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new BaseFunctionResolver(FuncParams
            .builder(Param.STRING, Param.INTEGER)
            .withVarArgs(Param.STRING).limitVarArgOccurrences(1)
            .build()) {

            @Override
            public FunctionImplementation getForTypes(List<DataType> datatypes) {
                return new LpadFunctionImpl(
                    new FunctionInfo(new FunctionIdent(NAME, datatypes), StringType.INSTANCE)
                );
            }
        });
    }

    private static class LpadFunctionImpl extends Scalar<String, String> {

        private final FunctionInfo info;

        LpadFunctionImpl(FunctionInfo info) {
            this.info = info;
        }

        @Override
        public FunctionInfo info() {
            return info;
        }

        @Override
        public String evaluate(TransactionContext txnCtx, Input[] args) {
            assert args.length >= 2 : "number of args must be 2 or 3";
            String toPad = (String) args[0].value(); // evaluate once
            Number length = (Number) args[1].value();
            if (toPad == null) {
                return null;
            }
            if (length == null) {
                return toPad;
            }
            String filler = null;
            if (args.length == 3) {
                filler = (String) args[2].value();
            }

            int len = length.intValue();
            final String fillerChars = (filler == null) ? " " : filler;
            int fillerLen = fillerChars.length();

            // Negative len is silently taken as zero
            if (len < 0) {
                return "";
            }

            if (toPad.length() > len) {
                return toPad.substring(0, len); /* truncate toPad to len chars */
            }

            if (fillerLen <= 0) {
                return toPad; /* nothing to pad with, so don't pad */
            }

            int toPadLen = len - toPad.length();

            StringBuilder ret = new StringBuilder(len);

            Collections.nCopies(toPadLen / fillerLen, fillerLen)
                .forEach(n -> ret.append(fillerChars));

            // rest
            ret.append(fillerChars.substring(0, toPadLen % fillerLen));

            ret.append(toPad);

            return ret.toString();
        }
    }

}
