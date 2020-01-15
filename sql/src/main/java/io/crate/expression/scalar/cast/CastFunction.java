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

import io.crate.data.Input;
import io.crate.exceptions.ConversionException;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.FunctionFormatSpec;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.types.CollectionType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.UndefinedType;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static io.crate.expression.scalar.cast.CastFunctionResolver.CAST_SIGNATURES;
import static io.crate.expression.scalar.cast.CastFunctionResolver.TRY_CAST_PREFIX;
import static io.crate.expression.symbol.format.SymbolPrinter.Strings.PAREN_CLOSE;
import static io.crate.expression.symbol.format.SymbolPrinter.Strings.PAREN_OPEN;

public class CastFunction extends Scalar<Object, Object> implements FunctionFormatSpec {

    private static final String TRY_CAST_SQL_NAME = "try_cast";
    private static final String CAST_SQL_NAME = "cast";

    private final DataType<?> returnType;
    private final FunctionInfo info;
    private final BiFunction<Symbol, DataType<?>, Symbol> onNormalizeException;
    private final BiFunction<Object, DataType<?>, Object> onEvaluateException;

    private CastFunction(FunctionInfo info,
                         BiFunction<Symbol, DataType<?>, Symbol> onNormalizeException,
                         BiFunction<Object, DataType<?>, Object> onEvaluateException) {
        this.info = info;
        this.returnType = info.returnType();
        this.onNormalizeException = onNormalizeException;
        this.onEvaluateException = onEvaluateException;
    }

    @Override
    public Object evaluate(TransactionContext txnCtx, Input<Object>[] args) {
        Object value = args[0].value();
        try {
            return returnType.value(value);
        } catch (ClassCastException | IllegalArgumentException e) {
            return onEvaluateException.apply(value, returnType);
        }
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext txnCtx) {
        Symbol argument = symbol.arguments().get(0);
        if (argument.symbolType().isValueSymbol()) {
            Object value = ((Input) argument).value();
            try {
                return Literal.of(returnType, returnType.value(value));
            } catch (ClassCastException | IllegalArgumentException e) {
                return onNormalizeException.apply(argument, returnType);
            }
        }
        return symbol;
    }

    @Override
    public String beforeArgs(Function function) {
        if (function.info().ident().name().startsWith(TRY_CAST_PREFIX)) {
            return TRY_CAST_SQL_NAME + PAREN_OPEN;
        } else {
            return CAST_SQL_NAME + PAREN_OPEN;
        }
    }

    @Override
    public String afterArgs(Function function) {
        DataType dataType = function.valueType();
        if (DataTypes.isCollectionType(dataType)) {
            CollectionType collectionType = ((CollectionType) dataType);
            return " AS " + collectionType.getCollectionName() +
                   PAREN_OPEN + collectionType.innerType().getName() + PAREN_CLOSE
                   + PAREN_CLOSE;
        }
        return " AS " + dataType.getName() + PAREN_CLOSE;
    }

    @Override
    public boolean formatArgs(Function function) {
        return true;
    }

    private static class CastResolver extends BaseFunctionResolver {

        private final DataType<?> type;
        private final String name;

        private final BiFunction<Symbol, DataType<?>, Symbol> castOnNormalizeException =
            (argument, returnType) -> {
                throw new ConversionException(argument, returnType);
            };
        private final BiFunction<Object, DataType<?>, Object> castOnEvaluateException =
            (argument, returnType) -> {
                throw new ConversionException(argument, returnType);
            };

        CastResolver(DataType type, String name) {
            super(FuncParams.builder(Param.ANY)
                      .withVarArgs(Param.ANY)
                      .limitVarArgOccurrences(1)
                      .build());
            this.type = type;
            this.name = name;
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) {
            DataType<?> targetType;
            if (dataTypes.size() > 1) {
                var targetTypeViaArg = dataTypes.get(1);
                targetType = targetTypeViaArg.id() != UndefinedType.ID ? targetTypeViaArg : type;
            } else {
                targetType = type;
            }

            DataType<?> sourceType = dataTypes.get(0);
            if (!sourceType.isConvertableTo(targetType)) {
                throw new ConversionException(sourceType, targetType);
            }

            return new CastFunction(
                new FunctionInfo(new FunctionIdent(name, dataTypes), targetType),
                castOnNormalizeException,
                castOnEvaluateException
            );
        }
    }

    private static class TryCastResolver extends BaseFunctionResolver {

        private final DataType<?> type;
        private final String name;

        private final BiFunction<Symbol, DataType<?>, Symbol> castOnNormalizeException =
            (argument, returnType) -> Literal.NULL;
        private final BiFunction<Object, DataType<?>, Object> castOnEvaluateException =
            (argument, returnType) -> null;

        TryCastResolver(DataType type, String name) {
            super(FuncParams.builder(Param.ANY)
                      .withVarArgs(Param.ANY)
                      .limitVarArgOccurrences(1)
                      .build());
            this.type = type;
            this.name = name;
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) {
            DataType<?> targetType;
            if (dataTypes.size() > 1) {
                var targetTypeViaArg = dataTypes.get(1);
                targetType = targetTypeViaArg.id() != UndefinedType.ID ? targetTypeViaArg : type;
            } else {
                targetType = type;
            }

            return new CastFunction(
                new FunctionInfo(new FunctionIdent(name, dataTypes), targetType),
                castOnNormalizeException,
                castOnEvaluateException
            );
        }
    }

    public static void register(ScalarFunctionModule module) {
        // We still maintain the cast function to type mapping to stay
        // bwc by keeping the old `to_<type>` and `try_<type>` function signatures.
        //
        // We can drop the per type cast function already in 4.2. This change
        // would require handling the metadata for the places where the old
        // signature of the cast function were used, e.g. generated columns.
        for (Map.Entry<String, DataType> function : CAST_SIGNATURES.entrySet()) {
            module.register(
                function.getKey(),
                new CastResolver(function.getValue(), function.getKey()));

            var tryCastFunctionName = TRY_CAST_PREFIX + function.getKey();
            module.register(
                tryCastFunctionName,
                new TryCastResolver(function.getValue(), tryCastFunctionName));
        }
    }
}
