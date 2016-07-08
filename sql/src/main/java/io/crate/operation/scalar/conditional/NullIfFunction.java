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

import com.google.common.base.Preconditions;
import io.crate.analyze.symbol.Function;
import io.crate.metadata.DynamicFunctionResolver;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;

import java.util.List;

public class NullIfFunction extends ConditionalFunction {
    public final static String NAME = "nullif";

    private NullIfFunction(FunctionInfo info) {
        super(info);
    }

    @Override
    public Object evaluate(Input... args) {
        Object arg0Value = args[0].value();
        return arg0Value != null && arg0Value.equals(args[1].value()) ? null : arg0Value;
    }

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new Resolver());
    }

    static class Resolver implements DynamicFunctionResolver {
        @Override
        public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            Preconditions.checkArgument(dataTypes.size() == 2, String.format("invalid size of arguments, 2 expected"));
            return new NullIfFunction(createInfo(NAME, dataTypes));
        }
    }
}
