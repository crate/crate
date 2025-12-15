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
import java.io.UncheckedIOException;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.IntUnaryOperator;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;

import io.crate.execution.dml.ArrayIndexer;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ScopedRef;
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

public class NumNullTermsPerDocQuery extends NumTermsPerDocQuery {

    public static NumNullTermsPerDocQuery of(ScopedRef ref,
                                             Function<ColumnIdent, ScopedRef> getRef,
                                             IntPredicate matches) {
        if (!hasArrayLengthIndexAndSortedNumericDocValues(ref.valueType(), ref.hasDocValues())) {
            return null;
        }
        return new NumNullTermsPerDocQuery(ref, getRef, matches);
    }

    /**
     * NumNullTermsPerDocQuery requires array length indexes and doc-values that can count the number of non-null
     * terms per docs.
     * Therefore,
     * multidimensional arrays (dim > 1) which do not index array lengths,
     * 1D arrays with element types using SortedSetDocValues or doc-values disabled
     * cannot use this query.
     * In other words, only 1D arrays using SortedNumericDocValues are supported.
     */
    private static boolean hasArrayLengthIndexAndSortedNumericDocValues(DataType<?> type, boolean hasDocValues) {
        if (ArrayType.dimensions(type) != 1 || !hasDocValues) {
            return false;
        }
        type = ArrayType.unnest(type);
        switch (type.id()) {
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
                return true;
            case NumericType.ID: {
                NumericType numericType = (NumericType) type;
                Integer precision = numericType.numericPrecision();
                return (precision != null && precision <= NumericStorage.COMPACT_PRECISION);
            }
            case StringType.ID:
            case CharacterType.ID:
            case BitStringType.ID:
            case IpType.ID:
            default:
                return false;
        }
    }

    private static IntUnaryOperator getNumNullTermsPerDocFunction(LeafReader reader, ScopedRef ref, Function<ColumnIdent, ScopedRef> getRef) {
        return numValuesPerDocForSortedNumeric(reader, ref, getRef);
    }

    private static IntUnaryOperator numValuesPerDocForSortedNumeric(LeafReader reader, ScopedRef ref, Function<ColumnIdent, ScopedRef> getRef) {
        final SortedNumericDocValues numNonNullTerms;
        final SortedNumericDocValues numAllTerms;
        try {
            numNonNullTerms = DocValues.getSortedNumeric(reader, ref.storageIdent());
            numAllTerms = DocValues.getSortedNumeric(reader, ArrayIndexer.toArrayLengthFieldName(ref, getRef));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return doc -> {
            try {
                return numAllTerms.advanceExact(doc) && numNonNullTerms.advanceExact(doc) ?
                    Math.toIntExact(numAllTerms.nextValue() - numNonNullTerms.docValueCount()) : -1;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    private NumNullTermsPerDocQuery(ScopedRef ref,
                                    Function<ColumnIdent, ScopedRef> getRef,
                                    IntPredicate matches) {
        super(
            ref.storageIdent(),
            leafReaderContext -> getNumNullTermsPerDocFunction(leafReaderContext.reader(), ref, getRef),
            matches);
    }

    @Override
    public String toString(String field) {
        return "NumNullTermsPerDoc: " + column;
    }
}
