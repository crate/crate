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
import io.crate.core.collections.MapComparator;
import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.operation.operator.Operator;
import io.crate.planner.symbol.*;
import io.crate.types.BooleanType;
import io.crate.types.CollectionType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.*;

public abstract class AnyOperator<Op extends AnyOperator<?>> extends Operator<Object> {
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
     * @see io.crate.operation.operator.CmpOperator#compare(int)
     */
    protected abstract boolean compare(int comparisonResult);

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

        if (containsNullLiteral(symbol.arguments())) {
            return Literal.NULL;
        }

        if (anyNonLiterals(symbol.arguments())) {
            return symbol;
        }

        Literal collLiteral = (Literal) symbol.arguments().get(1);
        Object leftValue = ((Literal) symbol.arguments().get(0)).value();
        if (!DataTypes.isCollectionType(collLiteral.valueType())) {
            throw new IllegalArgumentException("invalid array expression");
        }
        Iterable<?> collectionIterable = collectionValueToIterable(collLiteral.value());
        Boolean result = doEvaluate(leftValue, collectionIterable);
        if (result == null) {
            return Literal.NULL;
        } else {
            return Literal.newLiteral(result);
        }
    }

    @SuppressWarnings("unchecked")
    protected Boolean doEvaluate(Object left, Iterable<?> rightIterable) {

        if (left instanceof Comparable) {
            for (Object elem : rightIterable) {
                if (elem == null) {
                    // ignore null values
                    continue;
                }
                assert (left.getClass().equals(elem.getClass()));

                if (compare(((Comparable) left).compareTo(elem))) {
                    return true;
                }
            }
        } else if (left instanceof Map) {
            for (Object elem : rightIterable) {
                if (compare(Objects.compare((Map) left, (Map) elem, MapComparator.getInstance()))) {
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

        Object value = args[0].value();
        Object collectionReference = args[1].value();

        if (collectionReference == null || value == null) {
            return null;
        }
        Iterable<?> rightIterable;
        try {
           rightIterable = collectionValueToIterable(collectionReference);
        } catch (IllegalArgumentException e) {
            return false;
        }
        return doEvaluate(value, rightIterable);
    }

    public static Iterable<?> collectionValueToIterable(Object collectionRef) throws IllegalArgumentException {
        if (collectionRef instanceof Object[]) {
            return Arrays.asList((Object[])collectionRef);
        } else if (collectionRef instanceof Collection) {
            return (Collection<?>)collectionRef;
        } else {
            throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "cannot cast %s to Iterable", collectionRef));
        }
    }

    public abstract static class AnyResolver implements DynamicFunctionResolver {

        public abstract FunctionImplementation<Function> newInstance(FunctionInfo info);

        public abstract String name();

        @Override
        public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            Preconditions.checkArgument(
                    dataTypes.size() == 2 &&
                            DataTypes.isCollectionType(dataTypes.get(1)) &&
                            ((CollectionType)dataTypes.get(1)).innerType().equals(dataTypes.get(0))
            );
            return newInstance(new FunctionInfo(new FunctionIdent(name(), dataTypes), BooleanType.INSTANCE));
        }
    }
}
