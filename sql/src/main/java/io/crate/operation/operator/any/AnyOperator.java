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

package io.crate.operation.operator.any;

import com.google.common.base.Preconditions;
import io.crate.DataType;
import io.crate.core.collections.MapComparator;
import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.operation.operator.Operator;
import io.crate.planner.symbol.*;

import java.util.*;

public abstract class AnyOperator<Op extends AnyOperator<?>> extends Operator<Object>
                                                             implements Scalar<Boolean, Object>,
                                                                        DynamicFunctionResolver {
    public static final String OPERATOR_PREFIX = "any_";

    /**
     * called inside {@link #normalizeSymbol(io.crate.planner.symbol.Function)}
     * in order to interpret the result of compareTo
     *
     * subclass has to implement this to evaluate the -1, 0, 1 to boolean
     * e.g. for Lt  -1 is true, 0 and 1 is false.
     *
     * @param comparisonResult the result of someLiteral.compareTo(otherLiteral)
     * @return true/false
     * @see {@linkplain io.crate.operation.operator.CmpOperator#compare(int)}
     */
    protected abstract boolean compare(int comparisonResult);

    /**
     * @return the name of the operator
     */
    protected abstract String name();

    /**
     * create a new instance of an AnyOperator subclass with a given {@linkplain io.crate.metadata.FunctionInfo}
     * as one cannot intantiate generic class arguments directly
     */
    protected abstract Op newInstance(FunctionInfo info);

    protected FunctionInfo functionInfo;

    protected AnyOperator() {}

    protected AnyOperator(FunctionInfo functionInfo) {
        this.functionInfo = functionInfo;
    }

    @Override
    public FunctionInfo info() {
        return functionInfo;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        assert (symbol != null);
        assert (symbol.arguments().size() == 2);

        Symbol left = symbol.arguments().get(0);
        Symbol right = symbol.arguments().get(1);

        if (containsNull(left, right)) {
            return Null.INSTANCE;
        }

        if (left.symbolType().isLiteral() && right.symbolType().isLiteral()) {
            Literal collLiteral = (Literal)right;

            Object leftValue = ((Literal)left).value();
            Iterable<?> rightIter;
            if (collLiteral.valueType().isSetType()) {
                rightIter = ((SetLiteral)collLiteral).value();
            } else if (collLiteral.valueType().isArrayType()) {
                rightIter = Arrays.asList(((ArrayLiteral)collLiteral).value());
            } else {
                // only arrays or sets supported
                throw new IllegalArgumentException("invalid array expression");
            }

            if (doEvaluate(leftValue, rightIter)) {
                return BooleanLiteral.TRUE;
            } else {
                return BooleanLiteral.FALSE;
            }
        }
        return symbol;
    }

    @SuppressWarnings("unchecked")
    private Boolean doEvaluate(Object right, Iterable<?> leftIterable) {
        boolean rightComparable = right instanceof Comparable;
        boolean rightIsMap = right instanceof Map;

        if (rightComparable) {
            for (Object elem : leftIterable) {
                assert (right.getClass().equals(elem.getClass()));

                if (compare(((Comparable) right).compareTo(elem))) {
                    return true;
                }
            }
        } else if (rightIsMap) {
            for (Object elem : leftIterable) {
                if (compare(Objects.compare((Map) right, (Map) elem, MapComparator.getInstance()))) {
                    return true;
                }
            }

        }
        return false;
    }

    @Override
    public Boolean evaluate(Input<Object>... args) {
        assert (args != null);
        assert (args.length == 2);
        assert args[0] != null;

        Object collectionReference = args[0].value();
        Object value = args[1].value();

        if (collectionReference == null || value == null) {
            return null;
        }
        Iterable<?> leftIterable;
        if (collectionReference instanceof Object[]) {
            leftIterable = Arrays.asList((Object[])collectionReference);
        } else if (collectionReference instanceof Set) {
            leftIterable = (Set<?>)collectionReference;
        } else {
            return false;
        }
        return doEvaluate(value, leftIterable);
    }

    @Override
    public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
        Preconditions.checkArgument(
                dataTypes.size() == 2 &&
                        dataTypes.get(0).isCollectionType() &&
                        dataTypes.get(0).elementType().equals(dataTypes.get(1))
        );
        return newInstance(new FunctionInfo(new FunctionIdent(name(), dataTypes), DataType.BOOLEAN));
    }
}
