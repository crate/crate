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


public final class RpadFunction {

    private static final String NAME = "rpad";

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new BaseFunctionResolver(FuncParams
            .builder(Param.STRING, Param.INTEGER)
            .withVarArgs(Param.STRING).limitVarArgOccurrences(1)
            .build()) {

            @Override
            public FunctionImplementation getForTypes(List<DataType> datatypes) {
                return new RpadFunctionImpl(
                    new FunctionInfo(new FunctionIdent(NAME, datatypes), StringType.INSTANCE)
                );
            }
        });
    }

    private static class RpadFunctionImpl extends Scalar<String, String> {

        private final FunctionInfo info;

        RpadFunctionImpl(FunctionInfo info) {
            this.info = info;
        }

        @Override
        public FunctionInfo info() {
            return info;
        }

        @Override
        public String evaluate(TransactionContext txnCtx, Input[] args) {
            assert args.length >= 2 : "number of args must be 2 or 3";
            if (args[0].value() == null) {
                return null;
            }
            if (args[1].value() == null) {
                return (String) args[0].value();
            }

            LpadRpadFunctionImplHelper helper = new LpadRpadFunctionImplHelper(args);
            helper.copyString();
            helper.pad();

            return helper.getRet().toString();
        }
    }

}
