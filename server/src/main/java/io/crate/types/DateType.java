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

import static io.crate.types.TimestampType.TIMESTAMP_PARSER;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentParser;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

import io.crate.Streamer;
import io.crate.common.StringUtils;
import io.crate.execution.dml.IndexDocumentBuilder;
import io.crate.execution.dml.ValueIndexer;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.NumericColumnReference;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.SysColumns;
import io.crate.statistics.ColumnStatsSupport;

public class DateType extends DataType<LocalDate>
    implements FixedWidthType, Streamer<LocalDate> {

    public static final int ID = 24;
    public static final String NAME = "date";
    public static final DateType INSTANCE = new DateType();
    public static final int TYPE_SIZE = (int) RamUsageEstimator.shallowSizeOfInstance(LocalDate.class);
    public static final int DAY_TO_MS = 86400000;

    // Date values are streamed as timestamp (in ms since epoch) to clients via HTTP
    // So our max/min values are smaller than LocalDate.MAX/MIN and we're not using
    public static final LocalDate MAX_NULL_SENTINEL = ofTimestamp(Long.MAX_VALUE);
    public static final LocalDate MIN_NULL_SENTINEL = ofTimestamp(Long.MIN_VALUE);
    public static final LocalDate MAX = MAX_NULL_SENTINEL.minusDays(1);
    public static final LocalDate MIN = MIN_NULL_SENTINEL.plusDays(1);

    /// @throws IllegalArgumentException
    private static LocalDate ensureInAllowedRange(LocalDate date) {
        if (date.compareTo(MAX) > 0) {
            throw new IllegalArgumentException("date (" + date + ") exceeds allowed max value (" + MAX + ")");
        } else if (date.compareTo(MIN) < 0) {
            throw new IllegalArgumentException("date (" + date + ") exceeds allowed min value (" + MIN + ")");
        }
        return date;
    }

    public static LocalDate ofTimestamp(long msValue) {
        long epochDay = msValue / DAY_TO_MS;
        return LocalDate.ofEpochDay(epochDay);
    }

    public static long toTimestamp(LocalDate date) {
        return date.toEpochDay() * DAY_TO_MS;
    }

    public static long plus(LocalDate date, Period interval) {
        DateTime dateTime = new DateTime(
            date.getYear(),
            date.getMonthValue(),
            date.getDayOfMonth(),
            0,
            0,
            DateTimeZone.UTC
        );
        return dateTime.plus(interval).toInstant().getMillis();
    }

    public static long minus(LocalDate date, Period interval) {
        DateTime dateTime = new DateTime(
            date.getYear(),
            date.getMonthValue(),
            date.getDayOfMonth(),
            0,
            0,
            DateTimeZone.UTC
        );
        return dateTime.minus(interval).toInstant().getMillis();
    }

    private static LocalDate of(String str) {
        try {
            TemporalAccessor dt = TIMESTAMP_PARSER.parseBest(str, LocalDate::from, LocalDateTime::from);
            return LocalDate.from(dt);
        } catch (DateTimeParseException ex) {
            long[] out = StringUtils.PARSE_LONG_BUFFER.get();
            if (StringUtils.tryParseLong(str, out)) {
                return ofTimestamp(out[0]);
            } else {
                throw new ClassCastException("Can't cast '" + str + "' to " + NAME);
            }
        }
    }

    private static final StorageSupport<LocalDate> STORAGE = new StorageSupport<>(true, true, new DateEqQuery()) {

        @Override
        public LocalDate decode(long input) {
            return LocalDate.ofEpochDay(input);
        }

        @Override
        public ValueIndexer<LocalDate> valueIndexer(RelationName table,
                                                    Reference ref,
                                                    Function<ColumnIdent, Reference> getRef) {
            return new DateIndexer(ref);
        }

        @Override
        public LocalDate decode(byte[] packedPoint) {
            long epochDay = NumericUtils.sortableBytesToLong(packedPoint, 0);
            return LocalDate.ofEpochDay(epochDay);
        }

        @Override
        public LuceneCollectorExpression<LocalDate> getLuceneExpression(Reference ref,
                                                                   Predicate<Reference> isParentIgnored) {
            return new DateColumnReference(ref.storageIdent());
        }

        @Override
        public LocalDate decode(DataType<LocalDate> type, XContentParser parser) throws IOException {
            return LocalDate.ofEpochDay(parser.longValue());
        }
    };

    static class DateColumnReference extends NumericColumnReference<LocalDate> {

        protected DateColumnReference(String luceneField) {
            super(luceneField);
        }

        @Override
        protected LocalDate convert(long input) {
            return LocalDate.ofEpochDay(input);
        }
    }

    static class DateIndexer implements ValueIndexer<LocalDate> {

        private final String name;
        private final Reference ref;

        public DateIndexer(Reference ref) {
            this.ref = ref;
            this.name = ref.storageIdent();
        }

        @Override
        public void indexValue(LocalDate value, IndexDocumentBuilder docBuilder) throws IOException {
            ensureInAllowedRange(value);
            long epochDay = value.toEpochDay();
            if (ref.hasDocValues() && ref.indexType() != IndexType.NONE) {
                docBuilder.addField(new LongField(name, epochDay, Field.Store.NO));
            } else {
                if (ref.indexType() != IndexType.NONE) {
                    docBuilder.addField(new LongPoint(name, epochDay));
                }
                if (ref.hasDocValues()) {
                    docBuilder.addField(new SortedNumericDocValuesField(name, epochDay));
                } else {
                    if (docBuilder.maybeAddStoredField()) {
                        docBuilder.addField(new StoredField(name, epochDay));
                    }
                    docBuilder.addField(new Field(
                        SysColumns.FieldNames.NAME,
                        name,
                        SysColumns.FieldNames.FIELD_TYPE));
                }
            }
            docBuilder.translogWriter().writeValue(epochDay);
        }

        @Override
        public String storageIdentLeafName() {
            return ref.storageIdentLeafName();
        }
    }

    static class DateEqQuery implements EqQuery<LocalDate> {

        @Override
        public Query termQuery(String field, LocalDate value, boolean hasDocValues, boolean isIndexed) {
            long epochDay = value.toEpochDay();
            if (isIndexed) {
                return LongPoint.newExactQuery(field, epochDay);
            }
            if (hasDocValues) {
                return SortedNumericDocValuesField.newSlowExactQuery(field, epochDay);
            }
            return null;
        }

        @Override
        public Query rangeQuery(String field,
                                LocalDate lowerTerm,
                                LocalDate upperTerm,
                                boolean includeLower,
                                boolean includeUpper,
                                boolean hasDocValues,
                                boolean isIndexed) {
            long lower = Long.MIN_VALUE;
            if (lowerTerm != null) {
                long epochDay = lowerTerm.toEpochDay();
                lower = includeLower ? epochDay : epochDay + 1;
            }
            long upper = Long.MAX_VALUE;
            if (upperTerm != null) {
                long epochDay = upperTerm.toEpochDay();
                upper = includeUpper ? epochDay : epochDay - 1;
            }
            if (isIndexed) {
                return LongPoint.newRangeQuery(field, lower, upper);
            }
            if (hasDocValues) {
                return SortedNumericDocValuesField.newSlowRangeQuery(field, lower, upper);
            }
            return null;
        }

        @Override
        public Query termsQuery(String field, List<LocalDate> nonNullValues, boolean hasDocValues, boolean isIndexed) {
            if (isIndexed) {
                long[] epochDays = nonNullValues.stream().mapToLong(LocalDate::toEpochDay).toArray();
                return LongPoint.newSetQuery(field, epochDays);
            }
            if (hasDocValues) {
                long[] epochDays = nonNullValues.stream().mapToLong(LocalDate::toEpochDay).toArray();
                return SortedNumericDocValuesField.newSlowSetQuery(field, epochDays);
            }
            return null;
        }
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.DATE;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public TypeSignature getTypeSignature() {
        return TypeSignature.DATE;
    }

    @Override
    public Streamer<LocalDate> streamer() {
        return this;
    }

    @Override
    public LocalDate implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        return switch (value) {
            case null -> null;
            case String str -> of(str);
            case Double d -> ofTimestamp((long) (d * 1000));
            case Float f -> ofTimestamp((long) (f * 1000));
            case Number n -> ofTimestamp(n.longValue());
            case LocalDate d -> ensureInAllowedRange(d);
            default -> throw new ClassCastException("Can't cast '" + value + "' to " + getName());
        };
    }

    @Override
    public LocalDate sanitizeValue(Object value) {
        return switch (value) {
            case null -> null;
            case Integer i -> LocalDate.ofEpochDay(i);
            case Long i -> LocalDate.ofEpochDay(i);
            case Number n -> ofTimestamp(n.longValue());
            default -> (LocalDate) value;
        };
    }

    @Override
    public int compare(LocalDate o1, LocalDate o2) {
        return o1.compareTo(o2);
    }

    @Override
    public LocalDate readValueFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            return null;
        }
        return ofTimestamp(in.readLong());
    }

    @Override
    public void writeValueTo(StreamOutput out, LocalDate v) throws IOException {
        out.writeBoolean(v == null);
        if (v != null) {
            out.writeLong(toTimestamp(v));
        }
    }

    @Override
    public int fixedSize() {
        return TYPE_SIZE;
    }

    @Override
    public StorageSupport<LocalDate> storageSupport() {
        return STORAGE;
    }

    @Override
    public long valueBytes(LocalDate value) {
        return TYPE_SIZE;
    }

    @Override
    public ColumnStatsSupport<LocalDate> columnStatsSupport() {
        return ColumnStatsSupport.singleValued(LocalDate.class, DateType.this);
    }
}
