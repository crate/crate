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

package io.crate.expression.operator;

import io.crate.data.Input;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.sql.tree.ComparisonExpression;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Collection;
import java.util.List;
import java.util.function.IntPredicate;

public final class AllOperator extends Operator<Object> {

    public static final String OPERATOR_PREFIX = "_all_";
    private final FunctionInfo functionInfo;
    private final IntPredicate cmp;
    private final DataType leftType;

    public static class Names {
        public static final String EQ = OPERATOR_PREFIX + ComparisonExpression.Type.EQUAL.getValue();
        public static final String GTE = OPERATOR_PREFIX + ComparisonExpression.Type.GREATER_THAN_OR_EQUAL.getValue();
        public static final String GT = OPERATOR_PREFIX + ComparisonExpression.Type.GREATER_THAN.getValue();
        public static final String LTE = OPERATOR_PREFIX + ComparisonExpression.Type.LESS_THAN_OR_EQUAL.getValue();
        public static final String LT = OPERATOR_PREFIX + ComparisonExpression.Type.LESS_THAN.getValue();
        public static final String NEQ = OPERATOR_PREFIX + ComparisonExpression.Type.NOT_EQUAL.getValue();
    }

    public static void register(OperatorModule module) {
        module.registerDynamicOperatorFunction(Names.EQ, new AllResolver(Names.EQ, result -> result == 0));
        module.registerDynamicOperatorFunction(Names.GTE, new AllResolver(Names.GTE, result -> result >= 0));
        module.registerDynamicOperatorFunction(Names.GT, new AllResolver(Names.GT, result -> result > 0));
        module.registerDynamicOperatorFunction(Names.LTE, new AllResolver(Names.LTE, result -> result <= 0));
        module.registerDynamicOperatorFunction(Names.LT, new AllResolver(Names.LT, result -> result < 0));
        module.registerDynamicOperatorFunction(Names.NEQ, new AllResolver(Names.NEQ, result -> result != 0));
    }

    public AllOperator(FunctionInfo functionInfo, IntPredicate cmp) {
        this.functionInfo = functionInfo;
        this.cmp = cmp;
        this.leftType = functionInfo.ident().argumentTypes().get(0);
    }

    @Override
    public Boolean evaluate(TransactionContext txnCtx, Input<Object>... args) {
        var leftValue = args[0].value();
        var rightValues = (Collection) args[1].value();
        if (leftValue == null || rightValues == null) {
            return null;
        }
        boolean anyNulls = false;
        int matches = 0;
        for (Object rightValue : rightValues) {
            if (rightValue == null) {
                anyNulls = true;
                matches++;
            } else if (cmp.test(leftType.compare(leftValue, rightValue))) {
                matches++;
            }
        }
        if (matches == rightValues.size()) {
            return anyNulls ? null : true;
        } else {
            return false;
        }
    }

    @Override
    public FunctionInfo info() {
        return functionInfo;
    }

    static class AllResolver extends BaseFunctionResolver {

        private final String name;
        private final IntPredicate cmpIsMatch;

        protected AllResolver(String name, IntPredicate cmpIsMatch) {
            super(FuncParams.builder(
                Param.ANY,
                Param.of(new ArrayType<>(DataTypes.UNDEFINED)).withInnerType(Param.ANY)
            ).build());
            this.name = name;
            this.cmpIsMatch = cmpIsMatch;
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> types) throws IllegalArgumentException {
            assert types.size() == 2 : "<val> <operator> ALL(<array>) expects 2 arguments";
            assert types.get(1) instanceof ArrayType : "second argument to ALL must be an array";
            assert types.get(0).equals(((ArrayType<?>) types.get(1)).innerType()) : "value argument to ALL must match the type of the array argument";

            return new AllOperator(
                new FunctionInfo(new FunctionIdent(name, types), DataTypes.BOOLEAN),
                cmpIsMatch
            );
        }
    }
}
