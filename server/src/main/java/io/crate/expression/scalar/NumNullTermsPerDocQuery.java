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
import org.apache.lucene.index.SortedNumericDocValues;

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

public class NumNullTermsPerDocQuery extends NumTermsPerDocQuery {

    public static boolean isSupportedType(DataType<?> type) {
        type = ArrayType.unnest(type);
        // The types indexed with SortedSetDocValues are not supported, i.e., String, Char, IP ...
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

    private static IntUnaryOperator getNumNullTermsPerDocFunction(LeafReader reader, Reference ref, Function<ColumnIdent, Reference> getRef) {
        assert isSupportedType(ArrayType.unnest(ref.valueType())) : "Unsupported element type provided to NumNullTermsPerDocQuery";
        return numValuesPerDocForSortedNumeric(reader, ref, getRef);
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
