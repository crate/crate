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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;

/** A {@link FieldMapper} for numeric types: byte, short, int, long, float and double. */
public class NumberFieldMapper extends FieldMapper {

    public static final FieldType FIELD_TYPE = new FieldType();

    static {
        FIELD_TYPE.setStored(false);
        FIELD_TYPE.freeze();
    }

    public static class Builder extends FieldMapper.Builder<Builder> {

        private final NumberType type;

        public Builder(String name, NumberType type) {
            super(name, FIELD_TYPE);
            this.type = type;
            builder = this;
        }

        @Override
        public Builder indexOptions(IndexOptions indexOptions) {
            throw new MapperParsingException(
                    "index_options not allowed in field [" + name + "] of type [" + type.typeName() + "]");
        }

        @Override
        public NumberFieldMapper build(BuilderContext context) {
            var mapper = new NumberFieldMapper(
                name,
                position,
                defaultExpression,
                fieldType,
                new NumberFieldType(buildFullName(context), type, indexed, hasDocValues),
                context.indexSettings(),
                copyTo
            );
            context.putPositionInfo(mapper, position);
            return mapper;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        final NumberType type;

        public TypeParser(NumberType type) {
            this.type = type;
        }

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node,
                                         ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name, type);
            TypeParsers.parseField(builder, name, node, parserContext);
            return builder;
        }
    }

    public enum NumberType {
        FLOAT("float") {
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
        DOUBLE("double") {
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
        BYTE("byte") {
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
            public List<Field> createFields(String name, Number value,
                                            boolean indexed, boolean docValued, boolean stored) {
                return INTEGER.createFields(name, value, indexed, docValued, stored);
            }

            @Override
            Number valueForSearch(Number value) {
                return value.byteValue();
            }
        },
        SHORT("short") {
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
            public List<Field> createFields(String name, Number value,
                                            boolean indexed, boolean docValued, boolean stored) {
                return INTEGER.createFields(name, value, indexed, docValued, stored);
            }

            @Override
            Number valueForSearch(Number value) {
                return value.shortValue();
            }
        },
        INTEGER("integer") {
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
        LONG("long") {
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

        NumberType(String name) {
            this.name = name;
        }

        /** Get the associated type name. */
        public final String typeName() {
            return name;
        }

        public abstract Number parse(XContentParser parser, boolean coerce) throws IOException;

        public abstract Number parse(Object value, boolean coerce);

        public abstract Number parsePoint(byte[] value);

        public abstract List<Field> createFields(String name,
                                                 Number value,
                                                 boolean indexed,
                                                 boolean docValued,
                                                 boolean stored);

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

    public static final class NumberFieldType extends MappedFieldType {

        private final NumberType type;

        public NumberFieldType(String name, NumberType type, boolean isSearchable, boolean hasDocValues) {
            super(name, isSearchable, hasDocValues);
            this.type = Objects.requireNonNull(type);
        }

        public NumberFieldType(String name, NumberType type) {
            this(name, type, true, true);
        }

        @Override
        public String typeName() {
            return type.name;
        }

    }

    private NumberFieldMapper(
            String simpleName,
            int position,
            @Nullable String defaultExpression,
            FieldType fieldType,
            MappedFieldType mappedFieldType,
            Settings indexSettings,
            CopyTo copyTo) {
        super(simpleName, position, defaultExpression, fieldType, mappedFieldType, indexSettings, copyTo);
    }

    @Override
    public NumberFieldType fieldType() {
        return (NumberFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return fieldType().type.typeName();
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
        if (parser.currentToken() == Token.VALUE_NULL) {
            value = null;
        } else if (parser.currentToken() == Token.VALUE_STRING
                && parser.textLength() == 0) {
            value = null;
        } else {
            numericValue = fieldType().type.parse(parser, true);
            value = numericValue;
        }

        if (value == null) {
            return;
        }

        if (numericValue == null) {
            numericValue = fieldType().type.parse(value, true);
        }

        boolean docValued = fieldType().hasDocValues();
        boolean stored = fieldType.stored();
        fields.addAll(fieldType().type.createFields(
            fieldType().name(),
            numericValue,
            fieldType().isSearchable(),
            docValued,
            stored
        ));
        if (docValued == false && (stored || fieldType().isSearchable())) {
            createFieldNamesField(context, fields);
        }
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        NumberFieldMapper m = (NumberFieldMapper) other;
        if (fieldType().type != m.fieldType().type) {
            conflicts.add(
                "mapper [" + name() + "] cannot be changed from type ["
                + fieldType().type.name
                + "] to ["
                + m.fieldType().type.name + "]"
            );
        }
    }
}
