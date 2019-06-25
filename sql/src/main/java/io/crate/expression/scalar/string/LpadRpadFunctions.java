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

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Chars;
import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.StringType;

import java.util.HashSet;
import java.util.List;


public final class LpadRpadFunctions {

    private static final String NAME = "lpad";

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new BaseFunctionResolver(FuncParams
            .builder(Param.STRING, Param.INTEGER)
            .withVarArgs(Param.STRING).limitVarArgOccurrences(1)
            .build()) {

            @Override
            public FunctionImplementation getForTypes(List<DataType> datatypes) {
                return new LpadFunction(
                    new FunctionInfo(new FunctionIdent(NAME, datatypes), StringType.INSTANCE)
                );
            }
        });
    }

    private static class LpadFunction extends Scalar<String, String> {

        private final FunctionInfo info;

        LpadFunction(FunctionInfo info) {
            this.info = info;
        }

        @Override
        public FunctionInfo info() {
            return info;
        }

        // /**
        //  * Called to return a OPTIMIZED version of a scalar implementation.
        //  *
        //  * The returned instance will ONLY BE USED in the context of a single query
        //  */
        // @Override
        // public Scalar<String, String> compile(List<Symbol> arguments) {
        //     // assert arguments.size() == 3 : "number of args must be 3";

        //     return this;
        // }

        @Override
        public String evaluate(TransactionContext txnCtx, Input[] args) {
            assert args.length >= 2 : "number of args must be 2 or 3";
            String string1 = (String) args[0].value();
            String string2 = " "; // postgres.bki:1050
            int len = ((Number) args[1].value()).intValue();
            if (args.length == 3) {
                string2 = (String) args[2].value();
            }

            // Negative len is silently taken as zero
            if (len < 0) {
                len = 0;
            }

            int s1len = string1.length();

            int s2len = string2.length();

            if (s1len > len) {
                s1len = len; /* truncate string1 to len chars */
            }

            if (s2len <= 0) {
                len = s1len; /* nothing to pad with, so don't pad */
            }

            int bytelen = len; // TODO unicode

            StringBuilder ret = new StringBuilder(bytelen);

            int m = len - s1len;

            int ptr2 = 0, ptr2start = 0;
            int ptr2end = ptr2 + s2len;

            while (m-- > 0) {
                ret.append(string2.charAt(ptr2));
                ptr2 += 1; // TODO unicode

                if (ptr2 == ptr2end) {
                    ptr2 = ptr2start;
                }
            }

            int ptr1 = 0;

            while (s1len-- > 0) {
                ret.append(string1.charAt(ptr1));
                ptr1 += 1; // TODO unicode
            }

            return ret.toString();
        }
    }

}
