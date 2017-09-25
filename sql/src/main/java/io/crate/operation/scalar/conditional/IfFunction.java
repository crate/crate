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

package io.crate.operation.scalar.conditional;

import io.crate.data.Input;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;

import java.util.List;

/**
 * Conditional If/Else function, CASE expressions can be converted to chain of {@link IfFunction}s.
 * It takes at most 3 arguments: condition, result, default.
 * The 3rd argument is optional. If left out, default result will be NULL.
 *
 * <pre>
 *
 *      A CASE expression like this:
 *
 *      CASE WHEN id = 0 THEN 'zero' WHEN id % 2 = 0 THEN 'even' ELSE 'odd' END
 *
 *      can result in:
 *
 *      if(id = 0, 'zero', if(id % 2 = 0, 'even', 'odd'))
 *
 * </pre>
 *
 *
 */
public class IfFunction extends Scalar<Object, Object> {

    public static final String NAME = "if";

    private final FunctionInfo info;

    private IfFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Object evaluate(Input... args) {
        Boolean condition = (Boolean) args[0].value();
        if (condition != null && condition) {
            return args[1].value();
        }
        if (args.length == 3) {
            return args[2].value();
        }

        return null;
    }

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new Resolver());
    }

    private static FunctionInfo createInfo(List<DataType> dataTypes) {
        // return type and default return type have already been matched
        DataType returnType = dataTypes.get(1);
        return new FunctionInfo(new FunctionIdent(NAME, dataTypes), returnType, FunctionInfo.Type.SCALAR);
    }

    private static class Resolver extends BaseFunctionResolver {

        public Resolver() {
            super(FuncParams.builder(Param.BOOLEAN, Param.ANY)
                .withVarArgs(Param.ANY).limitVarArgOccurrences(1)
                .build());
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            return new IfFunction(createInfo(dataTypes));
        }
    }
}
