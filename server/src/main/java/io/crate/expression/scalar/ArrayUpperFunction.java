/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.expression.scalar;

import static io.crate.expression.scalar.array.ArrayArgumentValidators.ensureInnerTypeIsNotUndefined;
import static io.crate.lucene.LuceneQueryBuilder.genericFunctionFilter;
import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;

import java.util.List;
import java.util.function.IntPredicate;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.jetbrains.annotations.Nullable;

import io.crate.data.Input;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.GtOperator;
import io.crate.expression.operator.GteOperator;
import io.crate.expression.operator.LtOperator;
import io.crate.expression.operator.LteOperator;
import io.crate.expression.operator.Operators;
import io.crate.expression.predicate.IsNullPredicate;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.lucene.LuceneQueryBuilder.Context;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import io.crate.types.TypeSignature;

public class ArrayUpperFunction extends Scalar<Integer, Object> {

    public static final String ARRAY_UPPER = "array_upper";
    public static final String ARRAY_LENGTH = "array_length";

    public static void register(Functions.Builder module) {
        for (var name : List.of(ARRAY_UPPER, ARRAY_LENGTH)) {
            module.add(
                Signature.scalar(
                        name,
                        TypeSignature.parse("array(E)"),
                        DataTypes.INTEGER.getTypeSignature(),
                        DataTypes.INTEGER.getTypeSignature()
                    ).withTypeVariableConstraints(typeVariable("E"))
                    .withFeature(Feature.DETERMINISTIC)
                    .withFeature(Feature.NULLABLE),
                ArrayUpperFunction::new
            );
        }
    }


    private ArrayUpperFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
        ensureInnerTypeIsNotUndefined(boundSignature.argTypes(), signature.getName().name());
    }

    @Override
    public Integer evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
        @SuppressWarnings("unchecked")
        List<Object> values = (List<Object>) args[0].value();
        Object dimensionArg = args[1].value();
        if (values == null || values.isEmpty() || dimensionArg == null) {
            return null;
        }
        int dimension = (int) dimensionArg;
        if (dimension <= 0) {
            return null;
        }
        return upperBound(values, dimension, 1);
    }

    /**
     * Recursively traverses all sub-arrays up to requestedDimension.
     * If arrayOrItem is not a List we reached the last possible dimension.
     * @param requestedDimension original dimension provided in array_upper. Guaranteed to be > 0 before the first call.
     * @param currentDimension <= requestedDimension on initial and further calls.
     */
    static final Integer upperBound(@Nullable Object arrayOrItem, int requestedDimension, int currentDimension) {
        if (arrayOrItem instanceof List dimensionArray) {
            // instanceof is null safe
            if (currentDimension == requestedDimension) {
                return dimensionArray.size();
            } else {
                int max = Integer.MIN_VALUE;
                for (Object object: dimensionArray) {
                    Integer upper = upperBound(object, requestedDimension, currentDimension + 1);
                    if (upper != null) {
                        max = Math.max(max, upper);
                    }
                }
                // if max is not updated, all elements at level currentDimension+1 are nulls, this and further dimensions don't exist.
                return max == Integer.MIN_VALUE ? null : max;
            }
        } else {
            // We are on the last dimension (array with regular non-array items)
            // but requested dimension size is not yet resolved and thus doesn't exist.
            return null;
        }
    }


    /**
     * <pre>
     * {@code
     *  array_length(arr, dim) > 0
     *      |                  |
     *    inner              parent
     * }
     * </pre>
     */
    @Override
    public Query toQuery(Function parent, Function arrayLength, Context context) {
        String parentName = parent.name();
        if (!Operators.COMPARISON_OPERATORS.contains(parentName)) {
            return null;
        }
        List<Symbol> parentArgs = parent.arguments();
        Symbol cmpSymbol = parentArgs.get(1);
        if (!(cmpSymbol instanceof Input)) {
            return null;
        }
        Number cmpNumber = (Number) ((Input<?>) cmpSymbol).value();
        assert cmpNumber != null
            : "If the second argument to a cmp operator is a null literal it should normalize to null";
        List<Symbol> arrayLengthArgs = arrayLength.arguments();
        Symbol arraySymbol = arrayLengthArgs.get(0);
        if (!(arraySymbol instanceof Reference)) {
            return null;
        }
        Symbol dimensionSymbol = arrayLengthArgs.get(1);
        if (!(dimensionSymbol instanceof Input)) {
            return null;
        }
        int dimension = ((Number) ((Input<?>) dimensionSymbol).value()).intValue();
        if (dimension != 1) {
            // Storage of the multidimensional arrays is not supported.
            return null;
        }
        Reference arrayRef = (Reference) arraySymbol;
        DataType<?> elementType = ArrayType.unnest(arrayRef.valueType());
        if (elementType.id() == ObjectType.ID || elementType.equals(DataTypes.GEO_SHAPE)) {
            // No doc-values for these, can't utilize doc-value-count
            return null;
        }
        int cmpVal = cmpNumber.intValue();

        // For numeric types all values are stored, so the doc-value-count represents the number of not-null values
        // Only unique values are stored for IP and TEXT types, so the doc-value-count represents the number of unique not-null  values
        // [a, a, a]
        //      -> docValueCount 1
        //      -> arrayLength   3
        // array_length([], 1)
        //      -> NULL
        //
        //  array_length(arr, 1) =  0       noMatch
        //  array_length(arr, 1) =  1       genericFunctionFilter
        //  array_length(arr, 1) >  0       exists
        //  array_length(arr, 1) >  1       genericFunctionFilter
        //  array_length(arr, 1) >  20      genericFunctionFilter
        //  array_length(arr, 1) >= 0       exists
        //  array_length(arr, 1) >= 1       docValueCount >= 1
        //  array_length(arr, 1) >= 20      genericFunctionFilter
        //  array_length(arr, 1) <  0       noMatch
        //  array_length(arr, 1) <  1       noMatch
        //  array_length(arr, 1) <  20      genericFunctionFilter
        //  array_length(arr, 1) <= 0       noMatch
        //  array_length(arr, 1) <= 1       genericFunctionFilter
        //  array_length(arr, 1) <= 20      genericFunctionFilter

        IntPredicate valueCountIsMatch = predicateForFunction(parentName, cmpVal);

        switch (parentName) {
            case EqOperator.NAME:
                if (cmpVal == 0) {
                    return new MatchNoDocsQuery("array_length([], 1) is NULL, so array_length([], 1) = 0 can't match");
                }
                return genericAndDocValueCount(parent, context, arrayRef, valueCountIsMatch);

            case GtOperator.NAME:
                if (cmpVal == 0) {
                    return IsNullPredicate.refExistsQuery(arrayRef, context, false);
                }
                return docValueCountOrGeneric(parent, context, arrayRef, valueCountIsMatch);

            case GteOperator.NAME:
                if (cmpVal == 0) {
                    return IsNullPredicate.refExistsQuery(arrayRef, context, false);
                } else if (cmpVal == 1) {
                    return NumTermsPerDocQuery.forRef(arrayRef, valueCountIsMatch);
                } else {
                    return genericFunctionFilter(parent, context);
                }

            case LtOperator.NAME:
                if (cmpVal == 0 || cmpVal == 1) {
                    return new MatchNoDocsQuery("array_length([], 1) is NULL, so array_length([], 1) < 0 or < 1 can't match");
                }
                return genericAndDocValueCount(parent, context, arrayRef, valueCountIsMatch);

            case LteOperator.NAME:
                if (cmpVal == 0) {
                    return new MatchNoDocsQuery("array_length([], 1) is NULL, so array_length([], 1) <= 0 can't match");
                }
                return genericAndDocValueCount(parent, context, arrayRef, valueCountIsMatch);

            default:
                throw new IllegalArgumentException("Illegal operator: " + parentName);
        }
    }

    private static Query docValueCountOrGeneric(Function parent,
                                                 LuceneQueryBuilder.Context context,
                                                 Reference arrayRef,
                                                 IntPredicate valueCountIsMatch) {
        BooleanQuery.Builder query = new BooleanQuery.Builder();
        query.setMinimumNumberShouldMatch(1);
        return query
            .add(
                NumTermsPerDocQuery.forRef(arrayRef, valueCountIsMatch),
                BooleanClause.Occur.SHOULD
            )
            .add(genericFunctionFilter(parent, context), BooleanClause.Occur.SHOULD)
            .build();
    }

    private static Query genericAndDocValueCount(Function parent,
                                                 LuceneQueryBuilder.Context context,
                                                 Reference arrayRef,
                                                 IntPredicate valueCountIsMatch) {
        return new BooleanQuery.Builder()
            .add(
                NumTermsPerDocQuery.forRef(arrayRef, valueCountIsMatch),
                BooleanClause.Occur.MUST
            )
            .add(genericFunctionFilter(parent, context), BooleanClause.Occur.FILTER)
            .build();
    }

    private static IntPredicate predicateForFunction(String cmpFuncName, int cmpValue) {
        switch (cmpFuncName) {
            case LtOperator.NAME:
                return x -> x < cmpValue;

            case LteOperator.NAME:
                return x -> x <= cmpValue;

            case GtOperator.NAME:
                return x -> x > cmpValue;

            case GteOperator.NAME:
                return x -> x >= cmpValue;

            case EqOperator.NAME:
                return x -> x == cmpValue;

            default:
                throw new IllegalArgumentException("Unknown comparison function: " + cmpFuncName);
        }
    }
}
