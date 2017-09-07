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

package io.crate.operation.scalar.cast;

import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Signature;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;

import java.util.List;
import java.util.Map;

public class TryCastScalarFunction extends CastFunction {

    private TryCastScalarFunction(FunctionInfo functionInfo) {
        super(functionInfo);
    }

    public static void register(ScalarFunctionModule module) {
        for (Map.Entry<DataType, String> function : CastFunctionResolver.tryFunctionsMap().entrySet()) {
            module.register(function.getValue(), new Resolver(function.getKey(), function.getValue()));
        }
    }

    private static class Resolver extends BaseFunctionResolver {

        private final String name;
        private final DataType dataType;

        protected Resolver(DataType dataType, String name) {
            super(Signature.SIGNATURES_SINGLE_ANY);
            this.name = name;
            this.dataType = dataType;
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            return new TryCastScalarFunction(new FunctionInfo(new FunctionIdent(name, dataTypes), dataType));
        }
    }

    @Override
    protected Symbol onNormalizeException(Symbol argument) {
        return Literal.NULL;
    }

    @Override
    protected Object onEvaluateException(Object argument) {
        // swallow exception
        return null;
    }
}
