/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.scalar;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.operation.Input;
import io.crate.operation.reference.doc.lucene.TokenCountCollectorExpression;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.types.CollectionType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class TokenCountFunction implements Scalar<Integer, Object> {

    public static final String NAME = "token_count";
    private final FunctionInfo info;

    protected TokenCountFunction(FunctionInfo info) {
        this.info = info;
    }

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new Resolver());
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    protected abstract int eval(@Nonnull Object value);

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        Symbol columnName = symbol.arguments().get(0);

        if (columnName instanceof Reference) {
            ReferenceIdent ident = ((Reference) columnName).info().ident();
            String schema = ident.tableIdent().schema();
            if (schema == null || schema.equals(DocSchemaInfo.NAME)) {
                return replaceWithTokenCountReference(ident);
            } else {
                return symbol;
            }
        }

        assert columnName.symbolType().isValueSymbol();
        Object value = ((Input) columnName).value();
        if (value == null) {
            return Literal.NULL;
        }
        return Literal.newLiteral(eval(value));
    }

    private Reference replaceWithTokenCountReference(ReferenceIdent ident) {
        ArrayList<String> newPath = Lists.newArrayList(ident.columnIdent().path());
        newPath.add(0, ident.columnIdent().name());
        return new Reference(new ReferenceInfo(
                new ReferenceIdent(ident.tableIdent(),
                        new ColumnIdent(TokenCountCollectorExpression.BASE_NAME, newPath)),
                RowGranularity.DOC, DataTypes.INTEGER)
        );
    }

    private static class SingleTokenCountFunction extends TokenCountFunction {

        public SingleTokenCountFunction(FunctionInfo info) {
            super(info);
        }

        @Override
        protected int eval(@Nonnull Object value) {
            return 1;
        }

        @Override
        public Integer evaluate(Input[] args) {
            assert args.length == 1;
            if (args[0].value() == null) {
                return null;
            }
            return 1;
        }
    }

    private static class MultiTokenCountFunction extends TokenCountFunction {
        public MultiTokenCountFunction(FunctionInfo info) {
            super(info);
        }

        @Override
        protected int eval(@Nonnull Object value) {
            if (value instanceof Object[]) {
                return ((Object[]) value).length;
            }
            return ((Collection) value).size();
        }

        @Override
        public Integer evaluate(Input[] args) {
            Object value = args[0].value();
            if (value == null) {
                return null;
            }
            return eval(value);
        }
    }

    private static class Resolver implements DynamicFunctionResolver {
        @Override
        public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            Preconditions.checkArgument(dataTypes.size() == 1);
            DataType dataType = dataTypes.get(0);

            if (dataType instanceof CollectionType) {
                return new MultiTokenCountFunction(new FunctionInfo(new FunctionIdent(NAME, dataTypes), DataTypes.INTEGER));
            } else {
                return new SingleTokenCountFunction(new FunctionInfo(new FunctionIdent(NAME, dataTypes), DataTypes.INTEGER));
            }
        }
    }
}
