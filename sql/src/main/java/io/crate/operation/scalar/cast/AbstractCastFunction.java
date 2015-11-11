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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolFormatter;
import io.crate.exceptions.ConversionException;
import io.crate.metadata.DynamicFunctionResolver;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.types.DataType;

import java.util.List;
import java.util.Locale;

public abstract class AbstractCastFunction<From, To> extends Scalar<To,From> {

    protected final DataType<To> returnType;
    protected final FunctionInfo info;

    protected AbstractCastFunction(FunctionInfo info) {
        this.info = info;
        this.returnType = info.returnType();
    }

    @Override
    public To evaluate(Input<From>... args) {
        assert args.length == 1 : "Number of arguments must be 1";
        From value = args[0].value();
        try {
            return returnType.value(value);
        } catch (ClassCastException | IllegalArgumentException | ConversionException e) {
            return onEvaluateException(value, e);
        }
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        assert symbol.arguments().size() == 1 : "Number of arguments must be 1";
        Symbol argument = symbol.arguments().get(0);
        if (argument.symbolType().isValueSymbol()) {
            Object value = ((Input) argument).value();
            try {
                return Literal.newLiteral(returnType, returnType.value(value));
            } catch (ClassCastException | IllegalArgumentException | ConversionException e) {
                return onNormalizeException(argument, e);
            }
        }
        return symbol;
    }

    protected Symbol onNormalizeException(Symbol argument, Throwable t) {
        throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "cannot cast %s to %s",
                        SymbolFormatter.format(argument), returnType), t);
    }

    protected To onEvaluateException(From argument, Throwable t) {
        Throwables.propagate(t);
        return null;
    }

    protected abstract static class Resolver implements DynamicFunctionResolver {

        protected final String name;
        protected final DataType dataType;

        protected Resolver(DataType dataType, String name) {
            this.name = name;
            this.dataType = dataType;
        }

        protected void checkPreconditions(List<DataType> dataTypes) {
            Preconditions.checkArgument(dataTypes.size() == 1,
                    "invalid size of arguments, 1 expected");
            DataType convertFrom = dataTypes.get(0);
            Preconditions.checkArgument(convertFrom.isConvertableTo(dataType), "type '%s' not supported for conversion to '%s'", convertFrom, dataType);
        }

        protected abstract FunctionImplementation<Function> createInstance(List<DataType> types);

        @Override
        public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            checkPreconditions(dataTypes);
            return createInstance(dataTypes);
        }
    }
}
