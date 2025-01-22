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

import java.io.IOException;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.IntUnaryOperator;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

import io.crate.execution.dml.ArrayIndexer;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.types.ArrayType;
import io.crate.types.BitStringType;
import io.crate.types.BooleanType;
import io.crate.types.ByteType;
import io.crate.types.CharacterType;
import io.crate.types.DataType;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.GeoPointType;
import io.crate.types.IntegerType;
import io.crate.types.IpType;
import io.crate.types.LongType;
import io.crate.types.NumericStorage;
import io.crate.types.NumericType;
import io.crate.types.ShortType;
import io.crate.types.StringType;
import io.crate.types.TimestampType;

public class NumNullTermsPerDocQuery extends Query {

    private final Reference ref;
    private final java.util.function.Function<LeafReaderContext, IntUnaryOperator> NumNullTermsPerDocFactory;
    private final IntPredicate matches;

    private static IntUnaryOperator getNumNullTermsPerDocFunction(LeafReader reader, Reference ref, Function<ColumnIdent, Reference> getRef) {
        DataType<?> elementType = ArrayType.unnest(ref.valueType());
        switch (elementType.id()) {
            case BooleanType.ID:
            case ByteType.ID:
            case ShortType.ID:
            case IntegerType.ID:
            case LongType.ID:
            case TimestampType.ID_WITH_TZ:
            case TimestampType.ID_WITHOUT_TZ:
            case FloatType.ID:
            case DoubleType.ID:
            case GeoPointType.ID:
                return numValuesPerDocForSortedNumeric(reader, ref, getRef);

            case NumericType.ID: {
                NumericType numericType = (NumericType) elementType;
                Integer precision = numericType.numericPrecision();
                if (precision == null || precision > NumericStorage.COMPACT_PRECISION) {
                    return numValuesPerDocForSortedSetDocValues(reader, ref, getRef);
                }
                return numValuesPerDocForSortedNumeric(reader, ref, getRef);
            }

            case StringType.ID:
            case CharacterType.ID:
            case BitStringType.ID:
            case IpType.ID:
                return numValuesPerDocForSortedSetDocValues(reader, ref, getRef);

            default:
                throw new UnsupportedOperationException("NYI: " + elementType);
        }
    }

    private static IntUnaryOperator numValuesPerDocForSortedSetDocValues(LeafReader reader, Reference ref, Function<ColumnIdent, Reference> getRef) {
        final SortedSetDocValues numNonNullTerms;
        final SortedNumericDocValues numAllTerms;
        try {
            numNonNullTerms = DocValues.getSortedSet(reader, ref.storageIdent());
            numAllTerms = DocValues.getSortedNumeric(reader, ArrayIndexer.toArrayLengthFieldName(ref, getRef));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return doc -> {
            try {
                return numAllTerms.advanceExact(doc) && numNonNullTerms.advanceExact(doc) ?
                    Math.toIntExact(numAllTerms.nextValue()) - numNonNullTerms.docValueCount() : -1;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private static IntUnaryOperator numValuesPerDocForSortedNumeric(LeafReader reader, Reference ref, Function<ColumnIdent, Reference> getRef) {
        final SortedNumericDocValues numNonNullTerms;
        final SortedNumericDocValues numAllTerms;
        try {
            numNonNullTerms = DocValues.getSortedNumeric(reader, ref.storageIdent());
            numAllTerms = DocValues.getSortedNumeric(reader, ArrayIndexer.toArrayLengthFieldName(ref, getRef));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return doc -> {
            try {
                return numAllTerms.advanceExact(doc) && numNonNullTerms.advanceExact(doc) ?
                    Math.toIntExact(numAllTerms.nextValue()) - numNonNullTerms.docValueCount() : -1;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    public NumNullTermsPerDocQuery(Reference ref,
                                   Function<ColumnIdent, Reference> getRef,
                                   IntPredicate matches) {
        this.ref = ref;
        this.NumNullTermsPerDocFactory = leafReaderContext -> getNumNullTermsPerDocFunction(leafReaderContext.reader(), ref, getRef);
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
                    new NumNullTermsPerDocQuery.NumNullTermsPerDocTwoPhaseIterator(context.reader(), NumNullTermsPerDocFactory.apply(context), matches));
            }
        };
    }

    @Override
    public void visit(QueryVisitor visitor) {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NumNullTermsPerDocQuery that = (NumNullTermsPerDocQuery) o;

        if (!NumNullTermsPerDocFactory.equals(that.NumNullTermsPerDocFactory)) return false;
        return matches.equals(that.matches);
    }

    @Override
    public int hashCode() {
        int result = NumNullTermsPerDocFactory.hashCode();
        result = 31 * result + matches.hashCode();
        return result;
    }

    @Override
    public String toString(String field) {
        return "NumNullTermsPerDoc: " + ref;
    }

    static class NumNullTermsPerDocTwoPhaseIterator extends TwoPhaseIterator {

        private final IntUnaryOperator NumNullTermsOfDoc;
        private final IntPredicate matches;

        NumNullTermsPerDocTwoPhaseIterator(LeafReader reader,
                                              IntUnaryOperator NumNullTermsOfDoc,
                                              IntPredicate matches) {
            super(DocIdSetIterator.all(reader.maxDoc()));
            this.NumNullTermsOfDoc = NumNullTermsOfDoc;
            this.matches = matches;
        }

        @Override
        public boolean matches() {
            int doc = approximation.docID();
            return matches.test(NumNullTermsOfDoc.applyAsInt(doc));
        }

        @Override
        public float matchCost() {
            // This is an arbitrary number;
            // It's less than what is used in GenericFunctionQuery to indicate that this check should be cheaper
            return 2;
        }
    }
}
