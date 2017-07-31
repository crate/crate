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

package io.crate.operation.scalar;

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.Input;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionResolver;
import io.crate.metadata.Scalar;
import io.crate.metadata.Signature;
import io.crate.metadata.TransactionContext;
import io.crate.types.DataType;
import io.crate.types.SingleColumnTableType;

import java.util.List;

/**
 * Wrapper function to ensure a collection from a TableType only returns one value.
 * {@see ExpressionAnalyzer}
 *
 * Queries like Select 1 where 1 = (Select 1) require exactly one result for the SubQuery.
 */
public class SingleValueFunction extends Scalar<Object, Object[]> {

    public static final String NAME = "single_value";
    private final FunctionInfo info;

    private static final FunctionResolver functionResolver = new SingleValueFunctionResolver();

    public static void register(ScalarFunctionModule mod) {
        mod.register(NAME, functionResolver);
    }

    SingleValueFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext transactionContext) {
        return evaluateIfLiterals(this, symbol);
    }

    @Override
    public Object evaluate(Input<Object[]>... args) {
        Object[] arg0Value = args[0].value();
        if (arg0Value == null) {
            return null;
        }
        switch (arg0Value.length) {
            case 0:
                return null;
            case 1:
                return arg0Value[0];
            default:
                throw new UnsupportedOperationException("Subquery returned more than 1 row when it shouldn't.");
        }
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    static class SingleValueFunctionResolver extends BaseFunctionResolver {

        SingleValueFunctionResolver() {
            super(Signature.of(Signature.ArgMatcher.ANY_SINGLE_COLUMN));
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            if (dataTypes.size() != 1) {
                throw new IllegalArgumentException(NAME + " needs exactly one parameter");
            }
            DataType dataType = dataTypes.get(0);
            if (dataType.id() != SingleColumnTableType.ID) {
                throw new IllegalArgumentException("First parameter must be a SingleColumnTableType");
            }
            DataType returnType = ((SingleColumnTableType) dataType).innerType();
            return new SingleValueFunction(
                new FunctionInfo(new FunctionIdent(NAME, dataTypes),
                // returns the type if the first parameter
                returnType
            ));
        }
    }
}
