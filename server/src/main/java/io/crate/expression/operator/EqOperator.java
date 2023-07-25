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

import static io.crate.lucene.LuceneQueryBuilder.genericFunctionFilter;
import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;

import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Uid;
import org.jetbrains.annotations.Nullable;

import io.crate.data.Input;
import io.crate.expression.scalar.NumTermsPerDocQuery;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.lucene.LuceneQueryBuilder.Context;
import io.crate.metadata.IndexType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.sql.tree.BitString;
import io.crate.types.ArrayType;
import io.crate.types.BitStringType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;
import io.crate.types.EqQuery;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.IpType;
import io.crate.types.LongType;
import io.crate.types.ObjectType;
import io.crate.types.StorageSupport;
import io.crate.types.StringType;
import io.crate.types.TypeSignature;

public final class EqOperator extends Operator<Object> {

    public static final String NAME = "op_=";

    public static final Signature SIGNATURE = Signature.scalar(
        NAME,
        TypeSignature.parse("E"),
        TypeSignature.parse("E"),
        Operator.RETURN_TYPE.getTypeSignature()
    ).withTypeVariableConstraints(typeVariable("E"));

    public static void register(OperatorModule module) {
        module.register(
            SIGNATURE,
            EqOperator::new
        );
    }

    private final Signature signature;
    private final BoundSignature boundSignature;
    private final DataType<Object> argType;

    @SuppressWarnings("unchecked")
    private EqOperator(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
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
    public Signature signature() {
        return signature;
    }

    @Override
    public BoundSignature boundSignature() {
        return boundSignature;
    }

    @Override
    public Query toQuery(Function function, Context context) {
        List<Symbol> args = function.arguments();
        if (!(args.get(0) instanceof Reference ref && args.get(1) instanceof Literal<?> literal)) {
            return null;
        }
        String fqn = ref.column().fqn();
        Object value = literal.value();
        if (value == null) {
            return new MatchNoDocsQuery("`" + fqn + "` = null is always null");
        }
        DataType<?> dataType = ref.valueType();
        if (dataType.id() != ObjectType.ID && dataType.id() != ArrayType.ID && ref.indexType() == IndexType.NONE) {
            throw new IllegalArgumentException(
                "Cannot search on field [" + fqn + "] since it is not indexed.");
        }
        return switch (dataType.id()) {
            case ObjectType.ID -> refEqObject(function, fqn, (ObjectType) dataType, (Map<String, Object>) value, context);
            case ArrayType.ID -> termsAndGenericFilter(
                function,
                ref.column().fqn(),
                ArrayType.unnest(dataType),
                (Collection<?>) value,
                context
            );
            default -> fromPrimitive(dataType, fqn, value);
        };
    }

    @Nullable
    @SuppressWarnings("unchecked")
    public static Query termsQuery(String column, DataType<?> type, Collection<?> values) {
        List<?> nonNullValues = values.stream().filter(Objects::nonNull).toList();
        if (nonNullValues.isEmpty()) {
            return null;
        }
        if (column.equals(DocSysColumns.ID.name())) {
            BytesRef[] bytesRefs = new BytesRef[nonNullValues.size()];
            for (int i = 0; i < bytesRefs.length; i++) {
                Object idObject = nonNullValues.get(i);
                bytesRefs[i] = Uid.encodeId(idObject.toString());
            }
            return new TermInSetQuery(column, bytesRefs);
        }
        return switch (type.id()) {
            case StringType.ID -> new TermInSetQuery(column, nonNullValues.stream().map(BytesRefs::toBytesRef).toList());
            case IntegerType.ID -> IntPoint.newSetQuery(column, (List<Integer>) nonNullValues);
            case LongType.ID -> LongPoint.newSetQuery(column, (List<Long>) nonNullValues);
            case FloatType.ID -> FloatPoint.newSetQuery(column, (List<Float>) nonNullValues);
            case DoubleType.ID -> DoublePoint.newSetQuery(column, (List<Double>) nonNullValues);
            case IpType.ID -> InetAddressPoint.newSetQuery(
                column,
                nonNullValues.stream().map(x -> InetAddresses.forString((String) x)).toArray(InetAddress[]::new)
            );
            case BitStringType.ID -> new TermInSetQuery(
                column,
                nonNullValues.stream().map(x -> new BytesRef(((BitString) x).bitSet().toByteArray())).toList()
            );
            default -> booleanShould(column, type, nonNullValues);
        };
    }

    @Nullable
    private static Query booleanShould(String column, DataType<?> type, Collection<?> values) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (var term : values) {
            builder.add(EqOperator.fromPrimitive(type, column, term), Occur.SHOULD);
        }
        return new ConstantScoreQuery(builder.build());
    }

    private static Query termsAndGenericFilter(Function function, String column, DataType<?> elementType, Collection<?> values, LuceneQueryBuilder.Context context) {
        MappedFieldType fieldType = context.getFieldTypeOrNull(column);
        if (fieldType == null) {
            if (elementType.id() == ObjectType.ID) {
                return null; // Fallback to generic filter on ARRAY(OBJECT)
            }
            // field doesn't exist, can't match
            return new MatchNoDocsQuery("column does not exist in this index");
        }

        BooleanQuery.Builder filterClauses = new BooleanQuery.Builder();
        Query genericFunctionFilter = genericFunctionFilter(function, context);
        if (values.isEmpty()) {
            // `arrayRef = []` - termsQuery would be null

            if (fieldType.hasDocValues() == false) {
                //  Cannot use NumTermsPerDocQuery if column store is disabled, for example, ARRAY(GEO_SHAPE).
                return genericFunctionFilter;
            }

            filterClauses.add(
                NumTermsPerDocQuery.forColumn(column, elementType, numDocs -> numDocs == 0),
                BooleanClause.Occur.MUST
            );
            // Still need the genericFunctionFilter to avoid a match where the array contains NULL values.
            // NULL values are not in the index.
            filterClauses.add(genericFunctionFilter, BooleanClause.Occur.MUST);
        } else {
            // wrap boolTermsFilter and genericFunction filter in an additional BooleanFilter to control the ordering of the filters
            // termsFilter is applied first
            // afterwards the more expensive genericFunctionFilter
            Query termsQuery = termsQuery(column, elementType, values);
            if (termsQuery == null) {
                return genericFunctionFilter;
            }
            filterClauses.add(termsQuery, BooleanClause.Occur.MUST);
            filterClauses.add(genericFunctionFilter, BooleanClause.Occur.MUST);
        }
        return filterClauses.build();
    }

    @SuppressWarnings("unchecked")
    public static Query fromPrimitive(DataType<?> type, String column, Object value) {
        if (column.equals(DocSysColumns.ID.name())) {
            return new TermQuery(new Term(column, Uid.encodeId((String) value)));
        }
        StorageSupport<?> storageSupport = type.storageSupport();
        EqQuery<?> eqQuery = storageSupport == null ? null : storageSupport.eqQuery();
        if (eqQuery == null) {
            return null;
        }
        return ((EqQuery<Object>) eqQuery).termQuery(column, value);
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
     **/
    private static Query refEqObject(Function eq,
                                     String fqn,
                                     ObjectType type,
                                     Map<String, Object> value,
                                     LuceneQueryBuilder.Context context) {
        BooleanQuery.Builder boolBuilder = new BooleanQuery.Builder();
        int preFilters = 0;
        for (Map.Entry<String, Object> entry : value.entrySet()) {
            String key = entry.getKey();
            DataType<?> innerType = type.innerType(key);
            if (innerType == null) {
                // could be a nested object or not part of meta data; skip pre-filtering
                continue;
            }
            String fqNestedColumn = fqn + '.' + key;
            Query innerQuery;
            if (DataTypes.isArray(innerType)) {
                innerQuery = termsAndGenericFilter(eq, fqNestedColumn, innerType, (Collection<?>) entry.getValue(), context);
            } else {
                innerQuery = fromPrimitive(innerType, fqNestedColumn, entry.getValue());
            }
            if (innerQuery == null) {
                continue;
            }

            preFilters++;
            boolBuilder.add(innerQuery, BooleanClause.Occur.MUST);
        }
        if (preFilters == value.size()) {
            return boolBuilder.build();
        } else {
            Query genericEqFilter = genericFunctionFilter(eq, context);
            if (preFilters == 0) {
                return genericEqFilter;
            } else {
                boolBuilder.add(genericFunctionFilter(eq, context), BooleanClause.Occur.FILTER);
                return boolBuilder.build();
            }
        }
    }
}
