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

package io.crate.expression.operator;

import static io.crate.common.collections.Lists.flattenUnique;
import static io.crate.lucene.LuceneQueryBuilder.genericFunctionFilter;
import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.Uid;
import org.jetbrains.annotations.Nullable;

import io.crate.data.Input;
import io.crate.execution.dml.ArrayIndexer;
import io.crate.expression.scalar.NumTermsPerDocQuery;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.LuceneQueryBuilder.Context;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.IndexType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.SysColumns;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.EqQuery;
import io.crate.types.ObjectType;
import io.crate.types.StorageSupport;
import io.crate.types.TypeSignature;
import io.crate.types.UndefinedType;

public final class EqOperator extends Operator<Object> {

    public static final String NAME = "op_=";

    public static final Signature SIGNATURE = Signature.builder(NAME, FunctionType.SCALAR)
        .argumentTypes(TypeSignature.parse("E"),
            TypeSignature.parse("E"))
        .returnType(Operator.RETURN_TYPE.getTypeSignature())
        .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
        .typeVariableConstraints(typeVariable("E"))
        .build();

    public static void register(Functions.Builder builder) {
        builder.add(SIGNATURE, EqOperator::new);
    }

    private final DataType<Object> argType;

    @SuppressWarnings("unchecked")
    private EqOperator(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
        this.argType = (DataType<Object>) boundSignature.argTypes().get(0);
    }

    @Override
    @SafeVarargs
    public final Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>... args) {
        assert args.length == 2 : "number of args must be 2";
        Object left = args[0].value();
        if (left == null) {
            return null;
        }
        Object right = args[1].value();
        if (right == null) {
            return null;
        }
        return argType.compare(left, right) == 0;
    }

    @Override
    public Query toQuery(Function function, Context context) {
        List<Symbol> args = function.arguments();
        if (!(args.get(0) instanceof Reference ref && args.get(1) instanceof Literal<?> literal)) {
            return null;
        }
        String fqn = ref.column().fqn();
        String storageIdentifier = ref.storageIdent();
        Object value = literal.value();
        if (value == null) {
            return new MatchNoDocsQuery("`" + fqn + "` = null is always null");
        }
        DataType<?> dataType = ref.valueType();
        return switch (dataType.id()) {
            case ObjectType.ID -> refEqObject(
                function,
                ref.column(),
                (ObjectType) dataType,
                (Map<String, Object>) value,
                context
            );
            case ArrayType.ID -> termsAndGenericFilter(
                function,
                storageIdentifier,
                dataType,
                (Collection<?>) value,
                context,
                ref.hasDocValues(),
                ref.indexType());
            default -> fromPrimitive(dataType, storageIdentifier, value, ref.hasDocValues(), ref.indexType());
        };
    }

    @Nullable
    @SuppressWarnings("unchecked")
    public static Query termsQuery(String column, DataType<?> type, Collection<?> values, boolean hasDocValues, IndexType indexType) {
        if (column.equals(SysColumns.ID.COLUMN.name())) {
            ArrayList<BytesRef> bytesRefs = new ArrayList<>(values.size());
            for (Object value : values) {
                if (value != null) {
                    bytesRefs.add(Uid.encodeId(value.toString()));
                }
            }
            return bytesRefs.isEmpty() ? null : new TermInSetQuery(column, bytesRefs);
        }
        List<?> nonNullValues = values.stream().filter(Objects::nonNull).toList();
        if (nonNullValues.isEmpty()) {
            return null;
        }
        StorageSupport<?> storageSupport = type.storageSupport();
        EqQuery<?> eqQuery = storageSupport == null ? null : storageSupport.eqQuery();
        if (eqQuery == null) {
            return booleanShould(column, type, nonNullValues, hasDocValues, indexType);
        }
        return ((EqQuery<Object>) eqQuery).termsQuery(column, (List<Object>) nonNullValues, hasDocValues, indexType != IndexType.NONE);
    }

    @Nullable
    private static Query booleanShould(String column,
                                       DataType<?> type,
                                       Collection<?> values,
                                       boolean hasDocValues,
                                       IndexType indexType) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (var term : values) {
            var fromPrimitive = EqOperator.fromPrimitive(type, column, term, hasDocValues, indexType);
            if (fromPrimitive == null) {
                return null;
            }
            builder.add(fromPrimitive, Occur.SHOULD);
        }
        return new ConstantScoreQuery(builder.build());
    }

    public static Function of(Symbol first, Symbol second) {
        return new Function(SIGNATURE, List.of(first, second), Operator.RETURN_TYPE);
    }

    private static Query termsAndGenericFilter(Function function,
                                               String column,
                                               DataType<?> arrayType,
                                               Collection<?> values,
                                               Context context,
                                               boolean hasDocValues,
                                               IndexType indexType) {
        return termsAndGenericFilter(
            function,
            column,
            arrayType,
            values,
            context,
            hasDocValues,
            indexType,
            Occur.MUST
        );
    }

    /**
     *
     * @param termQueryOccur Defines how the inner type termQuery is build to satisfy a possible negation of the
     *                       whole query. This is used for cases like IS DISTINCT FROM.
     */
    public static Query termsAndGenericFilter(Function function,
                                              String column,
                                              DataType<?> arrayType,
                                              Collection<?> values,
                                              Context context,
                                              boolean hasDocValues,
                                              IndexType indexType,
                                              Occur termQueryOccur) {
        var flatUniqueValues = flattenUnique(values);
        var canUseArrayLengthIndex = context.tableInfo().versionCreated().onOrAfter(Version.V_5_9_0);
        BooleanQuery.Builder filterClauses = new BooleanQuery.Builder();
        Query genericFunctionFilter = genericFunctionFilter(function, context);
        if (flatUniqueValues.isEmpty()) { // using flatUnique to catch nested empty arrays
            if (canUseArrayLengthIndex) {
                var arrayLengthTermQuery = ArrayIndexer.arrayLengthTermQuery(
                    context.tableInfo().getReference(column),
                    0,
                    context.tableInfo()::getReference);
                if (ArrayType.dimensions(arrayType) > 1) { // need generic function filter to differentiate [[]] and []
                    return new BooleanQuery.Builder()
                        .add(arrayLengthTermQuery, termQueryOccur)
                        .add(genericFunctionFilter, Occur.MUST)
                        .build();
                } else {
                    return termQueryOccur == Occur.MUST_NOT ? Queries.not(arrayLengthTermQuery) : arrayLengthTermQuery;
                }
            } else {
                // `arrayRef = []` - termsQuery would be null

                if (hasDocValues == false) {
                    //  Cannot use NumTermsPerDocQuery if column store is disabled, for example, ARRAY(GEO_SHAPE).
                    return genericFunctionFilter;
                }

                filterClauses.add(
                    NumTermsPerDocQuery.forColumn(column, arrayType, numDocs -> numDocs == 0),
                    termQueryOccur
                );
                // Still need the genericFunctionFilter to avoid a match where the array contains NULL values.
                // NULL values are not in the index.
                filterClauses.add(genericFunctionFilter, Occur.MUST);
            }
        } else {
            // There is no benefit of using array length queries for eqOperator on non-empty arrays,
            // see https://github.com/crate/crate/pull/16479#issuecomment-2310825868

            // wrap boolTermsFilter and genericFunction filter in an additional BooleanFilter to control the ordering of the filters
            // termsFilter is applied first
            // afterwards the more expensive genericFunctionFilter
            Query termsQuery = termsQuery(column, arrayType, flatUniqueValues, hasDocValues, indexType);
            if (termsQuery == null) {
                return genericFunctionFilter;
            }
            filterClauses.add(termsQuery, termQueryOccur);
            filterClauses.add(genericFunctionFilter, Occur.MUST);
        }
        return filterClauses.build();
    }

    @Nullable
    @SuppressWarnings("unchecked")
    public static Query fromPrimitive(DataType<?> type, String column, Object value, boolean hasDocValues, IndexType indexType) {
        if (column.equals(SysColumns.ID.COLUMN.name())) {
            return new TermQuery(new Term(column, Uid.encodeId((String) value)));
        }
        StorageSupport<?> storageSupport = type.storageSupport();
        EqQuery<?> eqQuery = storageSupport == null ? null : storageSupport.eqQuery();
        if (eqQuery == null) {
            return null;
        }
        return ((EqQuery<Object>) eqQuery).termQuery(column, value, hasDocValues, indexType != IndexType.NONE);
    }

    /**
     * Query for object columns that tries to utilize efficient termQueries for the objects children.
     * <pre>
     * {@code
     *      // If x and y are known columns
     *      o = {x=10, y=20}    -> o.x=10 and o.y=20
     *
     *      // Only x is known:
     *      o = {x=10, y=20}    -> o.x=10 and generic(o == {x=10, y=20})
     *
     *      // No column is known:
     *      o = {x=10, y=20}    -> generic(o == {x=10, y=20})
     * }
     * </pre>
     *
     * @param termQueryOccur Defines how each object children termQuery is build to satisfy a possible negation of the
     *                       whole query. This is used for cases like IS DISTINCT FROM.
     **/
    public static Query refEqObject(Function eq,
                                    ColumnIdent columnIdent,
                                    ObjectType type,
                                    Map<String, Object> value,
                                    Context context,
                                    Occur termQueryOccur) {
        BooleanQuery.Builder boolBuilder = new BooleanQuery.Builder();
        int preFilters = 0;
        for (Map.Entry<String, Object> entry : value.entrySet()) {
            String key = entry.getKey();
            DataType<?> innerType = type.innerType(key);
            if (innerType == UndefinedType.INSTANCE) {
                // could be a nested object or not part of meta data; skip pre-filtering
                continue;
            }
            ColumnIdent childColumn = columnIdent.getChild(key);
            var childRef = context.getRef(childColumn);
            var nestedStorageIdentifier = childRef != null ? childRef.storageIdent() : childColumn.fqn();
            Query innerQuery;
            if (DataTypes.isArray(innerType)) {
                innerQuery = termsAndGenericFilter(
                    eq, nestedStorageIdentifier, innerType, (Collection<?>) entry.getValue(), context, childRef.hasDocValues(), childRef.indexType(), termQueryOccur);
            } else {
                innerQuery = fromPrimitive(innerType, nestedStorageIdentifier, entry.getValue(), childRef.hasDocValues(), childRef.indexType());
                // instead of adding a MUST_NOT clause for the negation, we negate the innerQuery to also match null values
                if (termQueryOccur == Occur.MUST_NOT) {
                    innerQuery = Queries.not(innerQuery);
                }
            }
            if (innerQuery == null) {
                continue;
            }

            preFilters++;
            boolBuilder.add(innerQuery, Occur.MUST);
        }
        if (preFilters > 0 && preFilters == value.size()) {
            return boolBuilder.build();
        } else {
            Query genericEqFilter = genericFunctionFilter(eq, context);
            if (preFilters == 0) {
                return genericEqFilter;
            } else {
                boolBuilder.add(genericFunctionFilter(eq, context), Occur.FILTER);
                return boolBuilder.build();
            }
        }
    }

    private static Query refEqObject(Function eq,
                                    ColumnIdent columnIdent,
                                    ObjectType type,
                                    Map<String, Object> value,
                                    Context context) {
        return refEqObject(eq, columnIdent, type, value, context, Occur.MUST);
    }
}
