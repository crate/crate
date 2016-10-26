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
import com.google.common.collect.Lists;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.BooleanType;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;

/**
 * Conditional If/Else function, CASE expressions can be converted to chain of {@link IfFunction}s.
 * It takes at most 3 arguments: condition, result, default.
 * The 3rd argument is optional. If left out, default result will be NULL.
 *
 * <pre>
 *
 *      A CASE expression like this:
 *
 *      CASE WHEN id == 0 THEN 'zero' WHEN id % 2 == 0 THEN 'even' ELSE 'odd' END
 *
 *      can result in:
 *
 *      if(id == 0, 'zero', if(id % 2 == 0, 'even', 'odd'))
 *
 * </pre>
 *
 *
 */
public class IfFunction extends Scalar<Object, Object> {

    public final static String NAME = "if";

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

    /**
     * Create a chain of if functions by the given list of operands and results.
     *
     * @param operands              List of condition symbols, all must result in a boolean value.
     * @param results               List of result symbols to return if corresponding condition evaluates to true.
     * @param defaultValueSymbol    Default symbol to return if all conditions evaluates to false.
     * @return                      Returns the first {@link IfFunction} of the chain.
     */
    public static Symbol createChain(List<Symbol> operands, List<Symbol> results, @Nullable Symbol defaultValueSymbol) {
        Symbol lastSymbol = defaultValueSymbol;
        // process operands in reverse order
        for (int i = operands.size() -1; i >= 0; i-- ) {
            Symbol operand = operands.get(i);
            Symbol result = results.get(i);
            List<Symbol> arguments = Lists.newArrayList(operand, result);
            List<DataType> argumentTypes = Lists.newArrayList(operand.valueType(), result.valueType());
            if (lastSymbol != null) {
                arguments.add(lastSymbol);
                argumentTypes.add(lastSymbol.valueType());
            }
            lastSymbol = new Function(createInfo(argumentTypes), arguments);
        }
        return lastSymbol;
    }

    private static FunctionInfo createInfo(List<DataType> dataTypes) {
        DataType valueType = dataTypes.get(1);
        DataType returnType = valueType;
        if (dataTypes.size() == 3) {
            returnType = dataTypes.get(2);
            if (returnType.id() != valueType.id()) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "%s type of default result argument %s does not match type of results argument %s",
                    NAME, returnType, valueType));
            }
        }
        return new FunctionInfo(new FunctionIdent(NAME, dataTypes), returnType, FunctionInfo.Type.SCALAR,
            FunctionInfo.DETERMINISTIC_AND_LAZY_ATTRIBUTES);
    }

    private static class Resolver implements DynamicFunctionResolver {

        @Override
        public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            Preconditions.checkArgument(dataTypes.size() >= 2 && dataTypes.size() <= 3,
                "Invalid number of arguments, must be either 2 or 3");
            Preconditions.checkArgument(dataTypes.get(0).id() == BooleanType.ID,
                "Data type of first argument is expected to be a boolean");
            return new IfFunction(createInfo(dataTypes));
        }
    }
}
