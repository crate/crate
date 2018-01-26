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

package io.crate.expression.scalar.cast;

import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.FunctionFormatSpec;
import io.crate.data.Input;
import io.crate.exceptions.ConversionException;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.params.Param;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.types.DataType;

import java.util.List;
import java.util.Map;

import static io.crate.expression.symbol.format.SymbolPrinter.Strings.PAREN_CLOSE;
import static io.crate.expression.symbol.format.SymbolPrinter.Strings.PAREN_OPEN;

public class CastFunction extends Scalar<Object, Object> implements FunctionFormatSpec {

    private static final String TRY_CAST_SQL_NAME = "try_cast";
    private static final String CAST_SQL_NAME = "cast";

    protected final DataType returnType;
    protected final FunctionInfo info;

    CastFunction(FunctionInfo info) {
        this.info = info;
        this.returnType = info.returnType();
    }

    @SafeVarargs
    @Override
    public final Object evaluate(Input<Object>... args) {
        assert args.length == 1 : "Number of arguments must be 1";
        Object value = args[0].value();
        try {
            return returnType.value(value);
        } catch (ClassCastException | IllegalArgumentException e) {
            return onEvaluateException(value);
        }
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext transactionContext) {
        assert symbol.arguments().size() == 1 : "Number of arguments must be 1";
        Symbol argument = symbol.arguments().get(0);
        if (argument.symbolType().isValueSymbol()) {
            Object value = ((Input) argument).value();
            try {
                return Literal.of(returnType, returnType.value(value));
            } catch (ClassCastException | IllegalArgumentException e) {
                return onNormalizeException(argument);
            }
        }
        return symbol;
    }

    protected Symbol onNormalizeException(Symbol argument) {
        throw new ConversionException(argument, returnType);
    }

    protected Object onEvaluateException(Object argument) {
        throw new ConversionException(argument, returnType);
    }

    @Override
    public String beforeArgs(Function function) {
        if (function.info().ident().name().startsWith(CastFunctionResolver.TRY_CAST_PREFIX)) {
            return TRY_CAST_SQL_NAME + PAREN_OPEN;
        } else {
            return CAST_SQL_NAME + PAREN_OPEN;
        }
    }

    @Override
    public String afterArgs(Function function) {
        return " AS " + function.valueType().getName() + PAREN_CLOSE;
    }

    @Override
    public boolean formatArgs(Function function) {
        return true;
    }

    private static class Resolver extends BaseFunctionResolver {

        private final String name;
        private final DataType targetType;

        protected Resolver(DataType targetType, String name) {
            super(FuncParams.builder(Param.ANY).build());
            this.name = name;
            this.targetType = targetType;
        }

        void checkPreconditions(List<DataType> dataTypes) {
            DataType convertFrom = dataTypes.get(0);
            if (!convertFrom.isConvertableTo(targetType)) {
                throw new ConversionException(convertFrom, targetType);
            }
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            checkPreconditions(dataTypes);
            return new CastFunction(new FunctionInfo(new FunctionIdent(name, dataTypes), targetType));
        }
    }

    public static void register(ScalarFunctionModule module) {
        for (Map.Entry<DataType, String> function : CastFunctionResolver.FUNCTION_MAP.entrySet()) {
            module.register(function.getValue(), new Resolver(function.getKey(), function.getValue()));
        }
    }
}
