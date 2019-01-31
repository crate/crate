/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.HalfFloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;

/** A {@link FieldMapper} for numeric types: byte, short, int, long, float and double. */
public class NumberFieldMapper extends FieldMapper {

    public static final Setting<Boolean> COERCE_SETTING =
            Setting.boolSetting("index.mapping.coerce", true, Property.IndexScope);

    public static class Defaults {
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final Explicit<Boolean> COERCE = new Explicit<>(true, false);
    }

    public static class Builder extends FieldMapper.Builder<Builder, NumberFieldMapper> {

        private Boolean ignoreMalformed;
        private Boolean coerce;

        public Builder(String name, NumberType type) {
            super(name, new NumberFieldType(type), new NumberFieldType(type));
            builder = this;
        }

        public Builder ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed = ignoreMalformed;
            return builder;
        }

        /**
         * Numeric field types no longer support `index_options`
         */
        @Override
        protected boolean allowsIndexOptions() {
            return false;
        }

        protected Explicit<Boolean> ignoreMalformed(BuilderContext context) {
            if (ignoreMalformed != null) {
                return new Explicit<>(ignoreMalformed, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(IGNORE_MALFORMED_SETTING.get(context.indexSettings()), false);
            }
            return Defaults.IGNORE_MALFORMED;
        }

        public Builder coerce(boolean coerce) {
            this.coerce = coerce;
            return builder;
        }

        protected Explicit<Boolean> coerce(BuilderContext context) {
            if (coerce != null) {
                return new Explicit<>(coerce, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(COERCE_SETTING.get(context.indexSettings()), false);
            }
            return Defaults.COERCE;
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);
        }

        @Override
        public NumberFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new NumberFieldMapper(name, fieldType, defaultFieldType, ignoreMalformed(context),
                    coerce(context), context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        final NumberType type;

        public TypeParser(NumberType type) {
            this.type = type;
        }

        @Override
        public Mapper.Builder<?,?> parse(String name, Map<String, Object> node,
                                         ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name, type);
            TypeParsers.parseField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    builder.nullValue(type.parse(propNode, false));
                    iterator.remove();
                } else if (propName.equals("ignore_malformed")) {
                    builder.ignoreMalformed(nodeBooleanValue(propNode, name + ".ignore_malformed"));
                    iterator.remove();
                } else if (propName.equals("coerce")) {
                    builder.coerce(nodeBooleanValue(propNode, name + ".coerce"));
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public enum NumberType {
        HALF_FLOAT("half_float", NumericType.HALF_FLOAT) {
            @Override
            public Float parse(Object value, boolean coerce) {
                return (Float) FLOAT.parse(value, false);
            }

            @Override
            public Number parsePoint(byte[] value) {
                return HalfFloatPoint.decodeDimension(value, 0);
            }

            @Override
            public Float parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.floatValue(coerce);
            }

            @Override
            public Query termQuery(String field, Object value) {
                float v = parse(value, false);
                return HalfFloatPoint.newExactQuery(field, v);
            }

            @Override
            public Query termsQuery(String field, List<Object> values) {
                float[] v = new float[values.size()];
                for (int i = 0; i < values.size(); ++i) {
                    v[i] = parse(values.get(i), false);
                }
                return HalfFloatPoint.newSetQuery(field, v);
            }

            @Override
            public Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                             boolean includeLower, boolean includeUpper,
                             boolean hasDocValues) {
                float l = Float.NEGATIVE_INFINITY;
                float u = Float.POSITIVE_INFINITY;
                if (lowerTerm != null) {
                    l = parse(lowerTerm, false);
                    if (includeLower) {
                        l = HalfFloatPoint.nextDown(l);
                    }
                    l = HalfFloatPoint.nextUp(l);
                }
                if (upperTerm != null) {
                    u = parse(upperTerm, false);
                    if (includeUpper) {
                        u = HalfFloatPoint.nextUp(u);
                    }
                    u = HalfFloatPoint.nextDown(u);
                }
                Query query = HalfFloatPoint.newRangeQuery(field, l, u);
                if (hasDocValues) {
                    Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(field,
                            HalfFloatPoint.halfFloatToSortableShort(l),
                            HalfFloatPoint.halfFloatToSortableShort(u));
                    query = new IndexOrDocValuesQuery(query, dvQuery);
                }
                return query;
            }

            @Override
            public List<Field> createFields(String name, Number value,
                                            boolean indexed, boolean docValued, boolean stored) {
                List<Field> fields = new ArrayList<>();
                if (indexed) {
                    fields.add(new HalfFloatPoint(name, value.floatValue()));
                }
                if (docValued) {
                    fields.add(new SortedNumericDocValuesField(name,
                        HalfFloatPoint.halfFloatToSortableShort(value.floatValue())));
                }
                if (stored) {
                    fields.add(new StoredField(name, value.floatValue()));
                }
                return fields;
            }
        },
        FLOAT("float", NumericType.FLOAT) {
            @Override
            public Float parse(Object value, boolean coerce) {
                if (value instanceof Number) {
                    return ((Number) value).floatValue();
                }
                if (value instanceof BytesRef) {
                    value = ((BytesRef) value).utf8ToString();
                }
                return Float.parseFloat(value.toString());
            }

            @Override
            public Number parsePoint(byte[] value) {
                return FloatPoint.decodeDimension(value, 0);
            }

            @Override
            public Float parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.floatValue(coerce);
            }

            @Override
            public Query termQuery(String field, Object value) {
                float v = parse(value, false);
                return FloatPoint.newExactQuery(field, v);
            }

            @Override
            public Query termsQuery(String field, List<Object> values) {
                float[] v = new float[values.size()];
                for (int i = 0; i < values.size(); ++i) {
                    v[i] = parse(values.get(i), false);
                }
                return FloatPoint.newSetQuery(field, v);
            }

            @Override
            public Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                             boolean includeLower, boolean includeUpper,
                             boolean hasDocValues) {
                float l = Float.NEGATIVE_INFINITY;
                float u = Float.POSITIVE_INFINITY;
                if (lowerTerm != null) {
                    l = parse(lowerTerm, false);
                    if (includeLower == false) {
                        l = FloatPoint.nextUp(l);
                    }
                }
                if (upperTerm != null) {
                    u = parse(upperTerm, false);
                    if (includeUpper == false) {
                        u = FloatPoint.nextDown(u);
                    }
                }
                Query query = FloatPoint.newRangeQuery(field, l, u);
                if (hasDocValues) {
                    Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(field,
                            NumericUtils.floatToSortableInt(l),
                            NumericUtils.floatToSortableInt(u));
                    query = new IndexOrDocValuesQuery(query, dvQuery);
                }
                return query;
            }

            @Override
            public List<Field> createFields(String name, Number value,
                                            boolean indexed, boolean docValued, boolean stored) {
                List<Field> fields = new ArrayList<>();
                if (indexed) {
                    fields.add(new FloatPoint(name, value.floatValue()));
                }
                if (docValued) {
                    fields.add(new SortedNumericDocValuesField(name,
                        NumericUtils.floatToSortableInt(value.floatValue())));
                }
                if (stored) {
                    fields.add(new StoredField(name, value.floatValue()));
                }
                return fields;
            }
        },
        DOUBLE("double", NumericType.DOUBLE) {
            @Override
            public Double parse(Object value, boolean coerce) {
                return objectToDouble(value);
            }

            @Override
            public Number parsePoint(byte[] value) {
                return DoublePoint.decodeDimension(value, 0);
            }

            @Override
            public Double parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.doubleValue(coerce);
            }

            @Override
            public Query termQuery(String field, Object value) {
                double v = parse(value, false);
                return DoublePoint.newExactQuery(field, v);
            }

            @Override
            public Query termsQuery(String field, List<Object> values) {
                double[] v = new double[values.size()];
                for (int i = 0; i < values.size(); ++i) {
                    v[i] = parse(values.get(i), false);
                }
                return DoublePoint.newSetQuery(field, v);
            }

            @Override
            public Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                             boolean includeLower, boolean includeUpper,
                             boolean hasDocValues) {
                double l = Double.NEGATIVE_INFINITY;
                double u = Double.POSITIVE_INFINITY;
                if (lowerTerm != null) {
                    l = parse(lowerTerm, false);
                    if (includeLower == false) {
                        l = DoublePoint.nextUp(l);
                    }
                }
                if (upperTerm != null) {
                    u = parse(upperTerm, false);
                    if (includeUpper == false) {
                        u = DoublePoint.nextDown(u);
                    }
                }
                Query query = DoublePoint.newRangeQuery(field, l, u);
                if (hasDocValues) {
                    Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(field,
                            NumericUtils.doubleToSortableLong(l),
                            NumericUtils.doubleToSortableLong(u));
                    query = new IndexOrDocValuesQuery(query, dvQuery);
                }
                return query;
            }

            @Override
            public List<Field> createFields(String name, Number value,
                                            boolean indexed, boolean docValued, boolean stored) {
                List<Field> fields = new ArrayList<>();
                if (indexed) {
                    fields.add(new DoublePoint(name, value.doubleValue()));
                }
                if (docValued) {
                    fields.add(new SortedNumericDocValuesField(name,
                        NumericUtils.doubleToSortableLong(value.doubleValue())));
                }
                if (stored) {
                    fields.add(new StoredField(name, value.doubleValue()));
                }
                return fields;
            }
        },
        BYTE("byte", NumericType.BYTE) {
            @Override
            public Byte parse(Object value, boolean coerce) {
                double doubleValue = objectToDouble(value);

                if (doubleValue < Byte.MIN_VALUE || doubleValue > Byte.MAX_VALUE) {
                    throw new IllegalArgumentException("Value [" + value + "] is out of range for a byte");
                }
                if (!coerce && doubleValue % 1 != 0) {
                    throw new IllegalArgumentException("Value [" + value + "] has a decimal part");
                }

                if (value instanceof Number) {
                    return ((Number) value).byteValue();
                }

                return (byte) doubleValue;
            }

            @Override
            public Number parsePoint(byte[] value) {
                return INTEGER.parsePoint(value).byteValue();
            }

            @Override
            public Short parse(XContentParser parser, boolean coerce) throws IOException {
                int value = parser.intValue(coerce);
                if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
                    throw new IllegalArgumentException("Value [" + value + "] is out of range for a byte");
                }
                return (short) value;
            }

            @Override
            public Query termQuery(String field, Object value) {
                return INTEGER.termQuery(field, value);
            }

            @Override
            public Query termsQuery(String field, List<Object> values) {
                return INTEGER.termsQuery(field, values);
            }

            @Override
            public Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                             boolean includeLower, boolean includeUpper,
                             boolean hasDocValues) {
                return INTEGER.rangeQuery(field, lowerTerm, upperTerm, includeLower, includeUpper, hasDocValues);
            }

            @Override
            public List<Field> createFields(String name, Number value,
                                            boolean indexed, boolean docValued, boolean stored) {
                return INTEGER.createFields(name, value, indexed, docValued, stored);
            }

            @Override
            Number valueForSearch(Number value) {
                return value.byteValue();
            }
        },
        SHORT("short", NumericType.SHORT) {
            @Override
            public Short parse(Object value, boolean coerce) {
                double doubleValue = objectToDouble(value);

                if (doubleValue < Short.MIN_VALUE || doubleValue > Short.MAX_VALUE) {
                    throw new IllegalArgumentException("Value [" + value + "] is out of range for a short");
                }
                if (!coerce && doubleValue % 1 != 0) {
                    throw new IllegalArgumentException("Value [" + value + "] has a decimal part");
                }

                if (value instanceof Number) {
                    return ((Number) value).shortValue();
                }

                return (short) doubleValue;
            }

            @Override
            public Number parsePoint(byte[] value) {
                return INTEGER.parsePoint(value).shortValue();
            }

            @Override
            public Short parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.shortValue(coerce);
            }

            @Override
            public Query termQuery(String field, Object value) {
                return INTEGER.termQuery(field, value);
            }

            @Override
            public Query termsQuery(String field, List<Object> values) {
                return INTEGER.termsQuery(field, values);
            }

            @Override
            public Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                             boolean includeLower, boolean includeUpper,
                             boolean hasDocValues) {
                return INTEGER.rangeQuery(field, lowerTerm, upperTerm, includeLower, includeUpper, hasDocValues);
            }

            @Override
            public List<Field> createFields(String name, Number value,
                                            boolean indexed, boolean docValued, boolean stored) {
                return INTEGER.createFields(name, value, indexed, docValued, stored);
            }

            @Override
            Number valueForSearch(Number value) {
                return value.shortValue();
            }
        },
        INTEGER("integer", NumericType.INT) {
            @Override
            public Integer parse(Object value, boolean coerce) {
                double doubleValue = objectToDouble(value);

                if (doubleValue < Integer.MIN_VALUE || doubleValue > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException("Value [" + value + "] is out of range for an integer");
                }
                if (!coerce && doubleValue % 1 != 0) {
                    throw new IllegalArgumentException("Value [" + value + "] has a decimal part");
                }

                if (value instanceof Number) {
                    return ((Number) value).intValue();
                }

                return (int) doubleValue;
            }

            @Override
            public Number parsePoint(byte[] value) {
                return IntPoint.decodeDimension(value, 0);
            }

            @Override
            public Integer parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.intValue(coerce);
            }

            @Override
            public Query termQuery(String field, Object value) {
                if (hasDecimalPart(value)) {
                    return Queries.newMatchNoDocsQuery("Value [" + value + "] has a decimal part");
                }
                int v = parse(value, true);
                return IntPoint.newExactQuery(field, v);
            }

            @Override
            public Query termsQuery(String field, List<Object> values) {
                int[] v = new int[values.size()];
                int upTo = 0;

                for (int i = 0; i < values.size(); i++) {
                    Object value = values.get(i);
                    if (!hasDecimalPart(value)) {
                        v[upTo++] = parse(value, true);
                    }
                }

                if (upTo == 0) {
                    return Queries.newMatchNoDocsQuery("All values have a decimal part");
                }
                if (upTo != v.length) {
                    v = Arrays.copyOf(v, upTo);
                }
                return IntPoint.newSetQuery(field, v);
            }

            @Override
            public Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                             boolean includeLower, boolean includeUpper,
                             boolean hasDocValues) {
                int l = Integer.MIN_VALUE;
                int u = Integer.MAX_VALUE;
                if (lowerTerm != null) {
                    l = parse(lowerTerm, true);
                    // if the lower bound is decimal:
                    // - if the bound is positive then we increment it:
                    //      if lowerTerm=1.5 then the (inclusive) bound becomes 2
                    // - if the bound is negative then we leave it as is:
                    //      if lowerTerm=-1.5 then the (inclusive) bound becomes -1 due to the call to longValue
                    boolean lowerTermHasDecimalPart = hasDecimalPart(lowerTerm);
                    if ((lowerTermHasDecimalPart == false && includeLower == false) ||
                            (lowerTermHasDecimalPart && signum(lowerTerm) > 0)) {
                        if (l == Integer.MAX_VALUE) {
                            return new MatchNoDocsQuery();
                        }
                        ++l;
                    }
                }
                if (upperTerm != null) {
                    u = parse(upperTerm, true);
                    boolean upperTermHasDecimalPart = hasDecimalPart(upperTerm);
                    if ((upperTermHasDecimalPart == false && includeUpper == false) ||
                            (upperTermHasDecimalPart && signum(upperTerm) < 0)) {
                        if (u == Integer.MIN_VALUE) {
                            return new MatchNoDocsQuery();
                        }
                        --u;
                    }
                }
                Query query = IntPoint.newRangeQuery(field, l, u);
                if (hasDocValues) {
                    Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(field, l, u);
                    query = new IndexOrDocValuesQuery(query, dvQuery);
                }
                return query;
            }

            @Override
            public List<Field> createFields(String name, Number value,
                                            boolean indexed, boolean docValued, boolean stored) {
                List<Field> fields = new ArrayList<>();
                if (indexed) {
                    fields.add(new IntPoint(name, value.intValue()));
                }
                if (docValued) {
                    fields.add(new SortedNumericDocValuesField(name, value.intValue()));
                }
                if (stored) {
                    fields.add(new StoredField(name, value.intValue()));
                }
                return fields;
            }
        },
        LONG("long", NumericType.LONG) {
            @Override
            public Long parse(Object value, boolean coerce) {
                if (value instanceof Long) {
                    return (Long)value;
                }

                double doubleValue = objectToDouble(value);
                // this check does not guarantee that value is inside MIN_VALUE/MAX_VALUE because values up to 9223372036854776832 will
                // be equal to Long.MAX_VALUE after conversion to double. More checks ahead.
                if (doubleValue < Long.MIN_VALUE || doubleValue > Long.MAX_VALUE) {
                    throw new IllegalArgumentException("Value [" + value + "] is out of range for a long");
                }
                if (!coerce && doubleValue % 1 != 0) {
                    throw new IllegalArgumentException("Value [" + value + "] has a decimal part");
                }

                // longs need special handling so we don't lose precision while parsing
                String stringValue = (value instanceof BytesRef) ? ((BytesRef) value).utf8ToString() : value.toString();
                return Numbers.toLong(stringValue, coerce);
            }

            @Override
            public Number parsePoint(byte[] value) {
                return LongPoint.decodeDimension(value, 0);
            }

            @Override
            public Long parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.longValue(coerce);
            }

            @Override
            public Query termQuery(String field, Object value) {
                if (hasDecimalPart(value)) {
                    return Queries.newMatchNoDocsQuery("Value [" + value + "] has a decimal part");
                }
                long v = parse(value, true);
                return LongPoint.newExactQuery(field, v);
            }

            @Override
            public Query termsQuery(String field, List<Object> values) {
                long[] v = new long[values.size()];
                int upTo = 0;

                for (int i = 0; i < values.size(); i++) {
                    Object value = values.get(i);
                    if (!hasDecimalPart(value)) {
                        v[upTo++] = parse(value, true);
                    }
                }

                if (upTo == 0) {
                    return Queries.newMatchNoDocsQuery("All values have a decimal part");
                }
                if (upTo != v.length) {
                    v = Arrays.copyOf(v, upTo);
                }
                return LongPoint.newSetQuery(field, v);
            }

            @Override
            public Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                             boolean includeLower, boolean includeUpper,
                             boolean hasDocValues) {
                long l = Long.MIN_VALUE;
                long u = Long.MAX_VALUE;
                if (lowerTerm != null) {
                    l = parse(lowerTerm, true);
                    // if the lower bound is decimal:
                    // - if the bound is positive then we increment it:
                    //      if lowerTerm=1.5 then the (inclusive) bound becomes 2
                    // - if the bound is negative then we leave it as is:
                    //      if lowerTerm=-1.5 then the (inclusive) bound becomes -1 due to the call to longValue
                    boolean lowerTermHasDecimalPart = hasDecimalPart(lowerTerm);
                    if ((lowerTermHasDecimalPart == false && includeLower == false) ||
                            (lowerTermHasDecimalPart && signum(lowerTerm) > 0)) {
                        if (l == Long.MAX_VALUE) {
                            return new MatchNoDocsQuery();
                        }
                        ++l;
                    }
                }
                if (upperTerm != null) {
                    u = parse(upperTerm, true);
                    boolean upperTermHasDecimalPart = hasDecimalPart(upperTerm);
                    if ((upperTermHasDecimalPart == false && includeUpper == false) ||
                            (upperTermHasDecimalPart && signum(upperTerm) < 0)) {
                        if (u == Long.MIN_VALUE) {
                            return new MatchNoDocsQuery();
                        }
                        --u;
                    }
                }
                Query query = LongPoint.newRangeQuery(field, l, u);
                if (hasDocValues) {
                    Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(field, l, u);
                    query = new IndexOrDocValuesQuery(query, dvQuery);
                }
                return query;
            }

            @Override
            public List<Field> createFields(String name, Number value,
                                            boolean indexed, boolean docValued, boolean stored) {
                List<Field> fields = new ArrayList<>();
                if (indexed) {
                    fields.add(new LongPoint(name, value.longValue()));
                }
                if (docValued) {
                    fields.add(new SortedNumericDocValuesField(name, value.longValue()));
                }
                if (stored) {
                    fields.add(new StoredField(name, value.longValue()));
                }
                return fields;
            }
        };

        private final String name;
        private final NumericType numericType;

        NumberType(String name, NumericType numericType) {
            this.name = name;
            this.numericType = numericType;
        }

        /** Get the associated type name. */
        public final String typeName() {
            return name;
        }
        /** Get the associated numeric type */
        final NumericType numericType() {
            return numericType;
        }
        public abstract Query termQuery(String field, Object value);
        public abstract Query termsQuery(String field, List<Object> values);
        public abstract Query rangeQuery(String field, Object lowerTerm, Object upperTerm,
                                  boolean includeLower, boolean includeUpper,
                                  boolean hasDocValues);
        public abstract Number parse(XContentParser parser, boolean coerce) throws IOException;
        public abstract Number parse(Object value, boolean coerce);
        public abstract Number parsePoint(byte[] value);
        public abstract List<Field> createFields(String name, Number value, boolean indexed,
                                                 boolean docValued, boolean stored);
        Number valueForSearch(Number value) {
            return value;
        }

        /**
         * Returns true if the object is a number and has a decimal part
         */
        boolean hasDecimalPart(Object number) {
            if (number instanceof Number) {
                double doubleValue = ((Number) number).doubleValue();
                return doubleValue % 1 != 0;
            }
            if (number instanceof BytesRef) {
                number = ((BytesRef) number).utf8ToString();
            }
            if (number instanceof String) {
                return Double.parseDouble((String) number) % 1 != 0;
            }
            return false;
        }

        /**
         * Returns -1, 0, or 1 if the value is lower than, equal to, or greater than 0
         */
        double signum(Object value) {
            if (value instanceof Number) {
                double doubleValue = ((Number) value).doubleValue();
                return Math.signum(doubleValue);
            }
            if (value instanceof BytesRef) {
                value = ((BytesRef) value).utf8ToString();
            }
            return Math.signum(Double.parseDouble(value.toString()));
        }

        /**
         * Converts an Object to a double by checking it against known types first
         */
        private static double objectToDouble(Object value) {
            double doubleValue;

            if (value instanceof Number) {
                doubleValue = ((Number) value).doubleValue();
            } else if (value instanceof BytesRef) {
                doubleValue = Double.parseDouble(((BytesRef) value).utf8ToString());
            } else {
                doubleValue = Double.parseDouble(value.toString());
            }

            return doubleValue;
        }
    }

    public static final class NumberFieldType extends SimpleMappedFieldType {

        private final NumberType type;

        public NumberFieldType(NumberType type) {
            super();
            this.type = Objects.requireNonNull(type);
            setTokenized(false);
            setHasDocValues(true);
            setOmitNorms(true);
        }

        private NumberFieldType(NumberFieldType other) {
            super(other);
            this.type = other.type;
        }

        @Override
        public MappedFieldType clone() {
            return new NumberFieldType(this);
        }

        @Override
        public String typeName() {
            return type.name;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (hasDocValues()) {
                return new DocValuesFieldExistsQuery(name());
            } else {
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            }
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            failIfNotIndexed();
            Query query = type.termQuery(name(), value);
            if (boost() != 1f) {
                query = new BoostQuery(query, boost());
            }
            return query;
        }

        @Override
        public Query termsQuery(List values, QueryShardContext context) {
            failIfNotIndexed();
            Query query = type.termsQuery(name(), values);
            if (boost() != 1f) {
                query = new BoostQuery(query, boost());
            }
            return query;
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, QueryShardContext context) {
            failIfNotIndexed();
            Query query = type.rangeQuery(name(), lowerTerm, upperTerm, includeLower, includeUpper, hasDocValues());
            if (boost() != 1f) {
                query = new BoostQuery(query, boost());
            }
            return query;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new DocValuesIndexFieldData.Builder().numericType(type.numericType());
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            return type.valueForSearch((Number) value);
        }

        @Override
        public DocValueFormat docValueFormat(String format, DateTimeZone timeZone) {
            if (timeZone != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName()
                    + "] does not support custom time zones");
            }
            if (format == null) {
                return DocValueFormat.RAW;
            } else {
                return new DocValueFormat.Decimal(format);
            }
        }

        public Number parsePoint(byte[] value) {
            return type.parsePoint(value);
        }

        @Override
        public boolean equals(Object o) {
            if (super.equals(o) == false) {
                return false;
            }
            NumberFieldType that = (NumberFieldType) o;
            return type == that.type;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), type);
        }
    }

    private Explicit<Boolean> ignoreMalformed;

    private Explicit<Boolean> coerce;

    private NumberFieldMapper(
            String simpleName,
            MappedFieldType fieldType,
            MappedFieldType defaultFieldType,
            Explicit<Boolean> ignoreMalformed,
            Explicit<Boolean> coerce,
            Settings indexSettings,
            MultiFields multiFields,
            CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.ignoreMalformed = ignoreMalformed;
        this.coerce = coerce;
    }

    @Override
    public NumberFieldType fieldType() {
        return (NumberFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return fieldType.typeName();
    }

    @Override
    protected NumberFieldMapper clone() {
        return (NumberFieldMapper) super.clone();
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        XContentParser parser = context.parser();
        Object value;
        Number numericValue = null;
        if (context.externalValueSet()) {
            value = context.externalValue();
        } else if (parser.currentToken() == Token.VALUE_NULL) {
            value = null;
        } else if (coerce.value()
                && parser.currentToken() == Token.VALUE_STRING
                && parser.textLength() == 0) {
            value = null;
        } else {
            try {
                numericValue = fieldType().type.parse(parser, coerce.value());
            } catch (IllegalArgumentException e) {
                if (ignoreMalformed.value()) {
                    context.addIgnoredField(fieldType.name());
                    return;
                } else {
                    throw e;
                }
            }
            value = numericValue;
        }

        if (value == null) {
            value = fieldType().nullValue();
        }

        if (value == null) {
            return;
        }

        if (numericValue == null) {
            numericValue = fieldType().type.parse(value, coerce.value());
        }

        boolean indexed = fieldType().indexOptions() != IndexOptions.NONE;
        boolean docValued = fieldType().hasDocValues();
        boolean stored = fieldType().stored();
        fields.addAll(fieldType().type.createFields(fieldType().name(), numericValue, indexed, docValued, stored));
        if (docValued == false && (stored || indexed)) {
            createFieldNamesField(context, fields);
        }
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        super.doMerge(mergeWith, updateAllTypes);
        NumberFieldMapper other = (NumberFieldMapper) mergeWith;
        if (other.ignoreMalformed.explicit()) {
            this.ignoreMalformed = other.ignoreMalformed;
        }
        if (other.coerce.explicit()) {
            this.coerce = other.coerce;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field("ignore_malformed", ignoreMalformed.value());
        }
        if (includeDefaults || coerce.explicit()) {
            builder.field("coerce", coerce.value());
        }

        if (includeDefaults || fieldType().nullValue() != null) {
            builder.field("null_value", fieldType().nullValue());
        }
    }
}
