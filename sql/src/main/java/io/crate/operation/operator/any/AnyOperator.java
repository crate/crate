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

import io.crate.analyze.symbol.Function;
import io.crate.core.collections.MapComparator;
import io.crate.data.Input;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Signature;
import io.crate.metadata.TransactionContext;
import io.crate.operation.operator.Operator;
import io.crate.types.BooleanType;
import io.crate.types.CollectionType;
import io.crate.types.DataType;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

public abstract class AnyOperator extends Operator<Object> {

    public static final String OPERATOR_PREFIX = "any_";

    /*
     * Rewrite `op ANY` to `op` using the actual function names.
     *
     * E.g. `any_=` becomes `op_=`
     */
    public static String nameToNonAny(String functionName) {
        return Operator.PREFIX + functionName.substring(OPERATOR_PREFIX.length());
    }

    /**
     * called inside {@link #normalizeSymbol(Function, TransactionContext)}
     * in order to interpret the result of compareTo
     * <p>
     * subclass has to implement this to evaluate the -1, 0, 1 to boolean
     * e.g. for Lt  -1 is true, 0 and 1 is false.
     *
     * @param comparisonResult the result of someLiteral.compareTo(otherLiteral)
     * @return true/false
     * @see io.crate.operation.operator.CmpOperator#compare(int)
     */
    protected abstract boolean compare(int comparisonResult);

    protected FunctionInfo functionInfo;

    protected AnyOperator() {
    }

    protected AnyOperator(FunctionInfo functionInfo) {
        this.functionInfo = functionInfo;
    }

    @Override
    public FunctionInfo info() {
        return functionInfo;
    }

    @SuppressWarnings("unchecked")
    protected Boolean doEvaluate(Object left, Iterable<?> rightIterable) {
        boolean anyNulls = false;
        if (left instanceof Comparable) {
            for (Object elem : rightIterable) {
                if (elem == null) {
                    anyNulls = true;
                    continue;
                }
                assert left.getClass().equals(elem.getClass()) : "class of left must be equal to the class of right";

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
        return anyNulls ? null : false;
    }

    @Override
    public Boolean evaluate(Input<Object>... args) {
        assert args != null : "args must not be null";
        assert args.length == 2 : "number of args must be 2";
        assert args[0] != null : "1st argument must not be null";

        Object value = args[0].value();
        Object collectionReference = args[1].value();

        if (collectionReference == null || value == null) {
            return null;
        }
        Iterable<?> rightIterable;
        rightIterable = collectionValueToIterable(collectionReference);
        return doEvaluate(value, rightIterable);
    }

    public static Iterable<?> collectionValueToIterable(Object collectionRef) throws IllegalArgumentException {
        if (collectionRef instanceof Object[]) {
            return Arrays.asList((Object[]) collectionRef);
        } else if (collectionRef instanceof Collection) {
            return (Collection<?>) collectionRef;
        } else {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "cannot cast %s to Iterable", collectionRef));
        }
    }

    public abstract static class AnyResolver extends BaseFunctionResolver {

        AnyResolver() {
            super(Signature.of(Signature.ArgMatcher.ANY, Signature.ArgMatcher.ANY_COLLECTION));
        }

        public abstract FunctionImplementation newInstance(FunctionInfo info);

        public abstract String name();

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            checkArgument(((CollectionType) dataTypes.get(1)).innerType().equals(dataTypes.get(0)),
                "The inner type of the array/set passed to ANY must match its left expression");
            return newInstance(new FunctionInfo(new FunctionIdent(name(), dataTypes), BooleanType.INSTANCE));
        }
    }
}
