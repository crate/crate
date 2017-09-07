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

import com.google.common.collect.Lists;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.data.Input;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.Signature;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

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
        for (int i = operands.size() - 1 ; i >= 0; i--) {
            Symbol operand = operands.get(i);
            Symbol result = results.get(i);
            List<Symbol> arguments = Lists.newArrayList(operand, result);
            if (lastSymbol != null) {
                arguments.add(lastSymbol);
            }
            lastSymbol = createFunction(arguments);
        }
        return lastSymbol;
    }

    public static Function createFunction(List<Symbol> arguments) {
        return new Function(createInfo(Symbols.typeView(arguments)), arguments);
    }

    private static FunctionInfo createInfo(List<DataType> dataTypes) {
        DataType condition = dataTypes.get(0);
        if (condition.equals(DataTypes.BOOLEAN) == false) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Argument of CASE/WHEN must be type boolean, not type %s",
                condition.getName()));
        }

        DataType valueType = dataTypes.get(1);
        DataType returnType = valueType;
        if (dataTypes.size() == 3) {
            returnType = dataTypes.get(2);
            // checks if valueType is convertable to returnType
            if (valueType.id() != returnType.id() &&
                (!valueType.isConvertableTo(returnType)) &&
                (valueType != DataTypes.UNDEFINED) &&
                (returnType != DataTypes.UNDEFINED)) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "%s type of default result argument %s cannot be converted to type of results argument %s",
                    NAME, returnType, valueType));
            }

            // prefer numeric dataType as the function returnType when applicable
            if (!returnType.isNumeric()) {
                returnType = valueType;
            }
        }
        return new FunctionInfo(new FunctionIdent(NAME, dataTypes), returnType, FunctionInfo.Type.SCALAR);
    }

    private static class Resolver extends BaseFunctionResolver {

        public Resolver() {
            super(Signature.withLenientVarArgs(Signature.ArgMatcher.BOOLEAN, Signature.ArgMatcher.ANY));
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            return new IfFunction(createInfo(dataTypes));
        }
    }
}
