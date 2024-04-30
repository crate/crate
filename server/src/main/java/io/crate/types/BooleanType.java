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

package io.crate.types;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.Streamer;
import io.crate.common.collections.Lists;
import io.crate.execution.dml.BooleanIndexer;
import io.crate.execution.dml.ValueIndexer;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.statistics.ColumnStatsSupport;

public class BooleanType extends DataType<Boolean> implements Streamer<Boolean>, FixedWidthType {

    public static final int ID = 3;
    public static final BooleanType INSTANCE = new BooleanType();

    private static final EqQuery<Boolean> EQ_QUERY = new EqQuery<>() {

        private static BytesRef indexedValue(Boolean value) {
            if (value == null) {
                return null;
            }
            return value ? new BytesRef("T") : new BytesRef("F");
        }

        @Override
        public Query termQuery(String field, Boolean value, boolean hasDocValues, boolean isIndexed) {
            if (isIndexed) {
                return new TermQuery(new Term(field, indexedValue(value)));
            } else {
                assert hasDocValues == true : "hasDocValues must be true for Boolean types since 'columnstore=false' is not supported.";
                return SortedNumericDocValuesField.newSlowExactQuery(
                    field,
                    value ? 1 : 0);
            }
        }

        @Override
        public Query rangeQuery(String field,
                                Boolean lowerTerm,
                                Boolean upperTerm,
                                boolean includeLower,
                                boolean includeUpper,
                                boolean hasDocValues,
                                boolean isIndexed) {
            assert isIndexed || hasDocValues == true : "hasDocValues must be true for Boolean types since 'columnstore=false' is not supported.";

            if (upperTerm == null) {
                includeUpper = true;
                upperTerm = true;
            }
            if (lowerTerm == null) {
                includeLower = true;
                lowerTerm = false;
            }

            boolean matchTrue = true;
            boolean matchFalse = true;

            if (includeLower) {
                if (lowerTerm) {
                    matchFalse = false;
                }
            } else {
                if (lowerTerm) {
                    matchFalse = false;
                    matchTrue = false;
                } else {
                    matchFalse = false;
                }
            }
            if (includeUpper) {
                if (!upperTerm) {
                    matchTrue = false;
                }
            } else {
                if (!upperTerm) {
                    matchTrue = false;
                    matchFalse = false;
                } else {
                    matchTrue = false;
                }
            }

            if (matchTrue && matchFalse) {
                return new FieldExistsQuery(field);
            } else if (matchTrue) {
                return termQuery(field, Boolean.TRUE, hasDocValues, isIndexed);
            } else if (matchFalse) {
                return termQuery(field, Boolean.FALSE, hasDocValues, isIndexed);
            } else {
                return new MatchNoDocsQuery();
            }
        }

        @Override
        public Query termsQuery(String field, List<Boolean> nonNullValues, boolean hasDocValues, boolean isIndexed) {
            if (isIndexed) {
                return new TermInSetQuery(field, Lists.map(nonNullValues, v -> indexedValue(v)));
            } else {
                assert hasDocValues == true : "hasDocValues must be true for Boolean types since 'columnstore=false' is not supported.";
                return SortedNumericDocValuesField.newSlowSetQuery(
                    field,
                    nonNullValues.stream().mapToLong(v -> v ? 1 : 0).toArray());
            }
        }
    };

    private static final StorageSupport<Boolean> STORAGE = new StorageSupport<>(
            true,
            false,
            EQ_QUERY) {

        @Override
        public ValueIndexer<Boolean> valueIndexer(RelationName table,
                                                  Reference ref,
                                                  Function<ColumnIdent, Reference> getRef) {
            return new BooleanIndexer(ref);
        }
    };

    private BooleanType() {
    }

    private static final Map<String, Boolean> BOOLEAN_MAP = Map.ofEntries(
        Map.entry("f", false),
        Map.entry("false", false),
        Map.entry("n", false),
        Map.entry("no", false),
        Map.entry("off", false),
        Map.entry("0", false),
        Map.entry("t", true),
        Map.entry("true", true),
        Map.entry("y", true),
        Map.entry("yes", true),
        Map.entry("on", true),
        Map.entry("1", true)
    );

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.BOOLEAN;
    }

    @Override
    public String getName() {
        return "boolean";
    }

    @Override
    public Streamer<Boolean> streamer() {
        return this;
    }

    @Override
    public Boolean implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        if (value == null) {
            return null;
        } else if (value instanceof Boolean b) {
            return b;
        } else if (value instanceof String str) {
            return booleanFromString(str);
        } else if (value instanceof Number number) {
            return booleanFromNumber(number);
        } else {
            throw new ClassCastException("Can't cast '" + value + "' to " + getName());
        }
    }

    @Override
    public Boolean sanitizeValue(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Boolean b) {
            return b;
        } else {
            return booleanFromNumber((Number) value);
        }
    }

    private Boolean booleanFromString(String value) {
        String lowerValue = value.toLowerCase(Locale.ENGLISH);
        Boolean boolValue = BOOLEAN_MAP.get(lowerValue);
        if (boolValue == null) {
            throw new IllegalArgumentException("Can't convert \"" + value + "\" to boolean");
        } else {
            return boolValue;
        }
    }

    private Boolean booleanFromNumber(Number value) {
        if (value.doubleValue() > 0.0) {
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    @Override
    public int compare(Boolean val1, Boolean val2) {
        return Boolean.compare(val1, val2);
    }

    @Override
    public Boolean readValueFrom(StreamInput in) throws IOException {
        return in.readOptionalBoolean();
    }

    @Override
    public void writeValueTo(StreamOutput out, Boolean v) throws IOException {
        out.writeOptionalBoolean(v);
    }

    @Override
    public int fixedSize() {
        return 8;
    }

    @Override
    public long valueBytes(Boolean value) {
        return 8;
    }

    @Override
    public StorageSupport<Boolean> storageSupport() {
        return STORAGE;
    }

    @Override
    public ColumnStatsSupport<Boolean> columnStatsSupport() {
        return ColumnStatsSupport.singleValued(Boolean.class, BooleanType.this);
    }
}
