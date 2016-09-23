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

package io.crate.operation.reference.doc.lucene;

import io.crate.action.sql.query.SortSymbolVisitor;
import io.crate.analyze.OrderBy;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

public class LuceneMissingValue {

    private static final BytesRef MAX_TERM;

    static {
        BytesRefBuilder builder = new BytesRefBuilder();
        final char[] chars = Character.toChars(Character.MAX_CODE_POINT);
        builder.copyChars(chars, 0, chars.length);
        MAX_TERM = builder.toBytesRef();
    }

    public static Object missingValue(OrderBy orderBy, int orderIndex) {
        assert orderIndex <= orderBy.orderBySymbols().size();
        return missingValue(orderBy.reverseFlags()[orderIndex],
            orderBy.nullsFirst()[orderIndex],
            SortSymbolVisitor.LUCENE_TYPE_MAP.get(orderBy.orderBySymbols().get(orderIndex).valueType()));

    }

    /**
     * Calculates the missing Values as in {@link org.elasticsearch.index.fielddata.IndexFieldData}
     * The results in the {@link org.apache.lucene.search.ScoreDoc} contains this missingValues instead of nulls. Because we
     * need nulls in the result, it's necessary to check if a value is a missingValue.
     */
    public static Object missingValue(boolean reverseFlag, Boolean nullFirst, SortField.Type type) {
        boolean min = reverseFlag ^ (nullFirst != null ? nullFirst : reverseFlag);
        switch (type) {
            case INT:
            case LONG:
                return min ? Long.MIN_VALUE : Long.MAX_VALUE;
            case FLOAT:
                return min ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
            case DOUBLE:
                return min ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
            case STRING:
            case STRING_VAL:
                return min ? null : MAX_TERM;
            default:
                throw new UnsupportedOperationException("Unsupported reduced type: " + type);
        }
    }
}
