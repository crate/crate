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

package io.crate.lucene;

import io.crate.data.Input;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.GtOperator;
import io.crate.expression.operator.GteOperator;
import io.crate.expression.operator.LtOperator;
import io.crate.expression.operator.LteOperator;
import io.crate.expression.operator.Operators;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.types.BooleanType;
import io.crate.types.ByteType;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.GeoPointType;
import io.crate.types.IntegerType;
import io.crate.types.IpType;
import io.crate.types.LongType;
import io.crate.types.ObjectType;
import io.crate.types.ShortType;
import io.crate.types.StringType;
import io.crate.types.TimeType;
import io.crate.types.TimestampType;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.function.IntPredicate;
import java.util.function.IntUnaryOperator;

import static io.crate.lucene.LuceneQueryBuilder.genericFunctionFilter;

public final class ArrayLengthQuery implements InnerFunctionToQuery {

    /**
     * <pre>
     * {@code
     *  array_length(arr, dim) > 0
     *      |                  |
     *    inner              parent
     * }
     * </pre>
     */
    @Nullable
    @Override
    public Query apply(Function parent, Function arrayLength, LuceneQueryBuilder.Context context) {
        String parentName = parent.info().ident().name();
        if (!Operators.COMPARISON_OPERATORS.contains(parentName)) {
            return null;
        }
        List<Symbol> parentArgs = parent.arguments();
        Symbol cmpSymbol = parentArgs.get(1);
        if (!(cmpSymbol instanceof Input)) {
            return null;
        }
        Number cmpNumber = (Number) ((Input) cmpSymbol).value();
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
        int dimension = ((Number) ((Input) dimensionSymbol).value()).intValue();
        if (dimension != 1) {
            return null;
        }
        Reference arrayRef = (Reference) arraySymbol;
        DataType elementType = ArrayType.unnest(arrayRef.valueType());
        if (elementType.id() == ObjectType.ID || elementType.equals(DataTypes.GEO_SHAPE)) {
            // No doc-values for these, can't utilize doc-value-count
            return null;
        }
        int cmpVal = cmpNumber.intValue();

        // Only unique values are stored, so the doc-value-count represent the number of unique values
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
                    return Queries.newMatchNoDocsQuery("array_length([], 1) is NULL, so array_length([], 1) = 0 can't match");
                }
                return genericAndDocValueCount(parent, context, arrayRef, valueCountIsMatch);

            case GtOperator.NAME:
                if (cmpVal == 0) {
                    return existsQuery(context, arrayRef);
                }
                return genericAndDocValueCount(parent, context, arrayRef, valueCountIsMatch);

            case GteOperator.NAME:
                if (cmpVal == 0) {
                    return existsQuery(context, arrayRef);
                } else if (cmpVal == 1) {
                    return numTermsPerDocQuery(arrayRef, valueCountIsMatch);
                } else {
                    return genericFunctionFilter(parent, context);
                }

            case LtOperator.NAME:
                if (cmpVal == 0 || cmpVal == 1) {
                    return Queries.newMatchNoDocsQuery("array_length([], 1) is NULL, so array_length([], 1) < 0 or < 1 can't match");
                }
                return genericAndDocValueCount(parent, context, arrayRef, valueCountIsMatch);

            case LteOperator.NAME:
                if (cmpVal == 0) {
                    return Queries.newMatchNoDocsQuery("array_length([], 1) is NULL, so array_length([], 1) <= 0 can't match");
                }
                return genericAndDocValueCount(parent, context, arrayRef, valueCountIsMatch);

            default:
                throw new IllegalArgumentException("Illegal operator: " + parentName);
        }
    }

    private static Query genericAndDocValueCount(Function parent,
                                                 LuceneQueryBuilder.Context context,
                                                 Reference arrayRef,
                                                 IntPredicate valueCountIsMatch) {
        return new BooleanQuery.Builder()
            .add(
                numTermsPerDocQuery(arrayRef, valueCountIsMatch),
                BooleanClause.Occur.MUST
            )
            .add(genericFunctionFilter(parent, context), BooleanClause.Occur.FILTER)
            .build();
    }

    private static NumTermsPerDocQuery numTermsPerDocQuery(Reference arrayRef, IntPredicate valueCountIsMatch) {
        return new NumTermsPerDocQuery(
            arrayRef.column().fqn(),
            leafReaderContext -> getNumTermsPerDocFunction(leafReaderContext.reader(), arrayRef),
            valueCountIsMatch
        );
    }

    private static Query existsQuery(LuceneQueryBuilder.Context context, Reference arrayRef) {
        MappedFieldType fieldType = context.getFieldTypeOrNull(arrayRef.column().fqn());
        if (fieldType == null) {
            return Queries.newMatchNoDocsQuery("MappedFieldType missing");
        }
        return fieldType.existsQuery(context.queryShardContext());
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

    private static IntUnaryOperator getNumTermsPerDocFunction(LeafReader reader, Reference ref) {
        DataType elementType = ArrayType.unnest(ref.valueType());
        switch (elementType.id()) {
            case BooleanType.ID:
            case ByteType.ID:
            case ShortType.ID:
            case IntegerType.ID:
            case LongType.ID:
            case TimeType.ID:
            case TimestampType.ID_WITH_TZ:
            case TimestampType.ID_WITHOUT_TZ:
            case FloatType.ID:
            case DoubleType.ID:
            case GeoPointType.ID:
                return numValuesPerDocForSortedNumeric(reader, ref.column());

            case StringType.ID:
                return numValuesPerDocForString(reader, ref.column());

            case IpType.ID:
                return numValuesPerDocForIP(reader, ref.column());

            default:
                throw new UnsupportedOperationException("NYI: " + elementType);
        }
    }

    private static IntUnaryOperator numValuesPerDocForIP(LeafReader reader, ColumnIdent column) {
        SortedSetDocValues docValues;
        try {
            docValues = reader.getSortedSetDocValues(column.fqn());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return doc -> {
            try {
                if (docValues.advanceExact(doc)) {
                    int c = 1;
                    while (docValues.nextOrd() != SortedSetDocValues.NO_MORE_ORDS) {
                        c++;
                    }
                    return c;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return 0;
        };
    }

    private static IntUnaryOperator numValuesPerDocForString(LeafReader reader, ColumnIdent column) {
        SortedBinaryDocValues docValues;
        try {
            docValues = FieldData.toString(DocValues.getSortedSet(reader, column.fqn()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return doc -> {
            try {
                return docValues.advanceExact(doc) ? docValues.docValueCount() : 0;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private static IntUnaryOperator numValuesPerDocForSortedNumeric(LeafReader reader, ColumnIdent column) {
        final SortedNumericDocValues sortedNumeric;
        try {
            sortedNumeric = DocValues.getSortedNumeric(reader, column.fqn());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return doc -> {
            try {
                return sortedNumeric.advanceExact(doc) ? sortedNumeric.docValueCount() : 0;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    static class NumTermsPerDocQuery extends Query {

        private final String column;
        private final java.util.function.Function<LeafReaderContext, IntUnaryOperator> numTermsPerDocFactory;
        private final IntPredicate matches;

        NumTermsPerDocQuery(String column,
                            java.util.function.Function<LeafReaderContext, IntUnaryOperator> numTermsPerDocFactory,
                            IntPredicate matches) {
            this.column = column;
            this.numTermsPerDocFactory = numTermsPerDocFactory;
            this.matches = matches;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new ConstantScoreWeight(this, boost) {
                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return false;
                }

                @Override
                public Scorer scorer(LeafReaderContext context) {
                    return new ConstantScoreScorer(
                        this,
                        0f,
                        scoreMode,
                        new NumTermsPerDocTwoPhaseIterator(context.reader(), numTermsPerDocFactory.apply(context), matches));
                }
            };
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            NumTermsPerDocQuery that = (NumTermsPerDocQuery) o;

            if (!numTermsPerDocFactory.equals(that.numTermsPerDocFactory)) return false;
            return matches.equals(that.matches);
        }

        @Override
        public int hashCode() {
            int result = numTermsPerDocFactory.hashCode();
            result = 31 * result + matches.hashCode();
            return result;
        }

        @Override
        public String toString(String field) {
            return "NumTermsPerDoc: " + column;
        }
    }

    private static class NumTermsPerDocTwoPhaseIterator extends TwoPhaseIterator {

        private final IntUnaryOperator numTermsOfDoc;
        private final IntPredicate matches;

        NumTermsPerDocTwoPhaseIterator(LeafReader reader,
                                       IntUnaryOperator numTermsOfDoc,
                                       IntPredicate matches) {
            super(DocIdSetIterator.all(reader.maxDoc()));
            this.numTermsOfDoc = numTermsOfDoc;
            this.matches = matches;
        }

        @Override
        public boolean matches() {
            int doc = approximation.docID();
            return matches.test(numTermsOfDoc.applyAsInt(doc));
        }

        @Override
        public float matchCost() {
            // This is an arbitrary number;
            // It's less than what is used in GenericFunctionQuery to indicate that this check should be cheaper
            return 2;
        }
    }
}
