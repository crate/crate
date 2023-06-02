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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatField;
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
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.jetbrains.annotations.Nullable;

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
                columnOID,
                isDropped,
                defaultExpression,
                fieldType,
                new NumberFieldType(buildFullName(context), type, indexed, hasDocValues),
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
        public Mapper.Builder<?> parse(String name,
                                       Map<String, Object> node,
                                       ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name, type);
            TypeParsers.parseField(builder, name, node);
            return builder;
        }
    }

    public enum NumberType {
        FLOAT("float") {
            @Override
            public Float parse(Object value, boolean coerce) {
                if (value instanceof Number number) {
                    return number.floatValue();
                }
                if (value instanceof BytesRef bytesRef) {
                    value = bytesRef.utf8ToString();
                }
                return Float.parseFloat(value.toString());
            }

            @Override
            public Float parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.floatValue(coerce);
            }

            @Override
            public void createFields(Consumer<IndexableField> onField,
                                     String name,
                                     Number value,
                                     boolean indexed,
                                     boolean docValued,
                                     boolean stored) {
                if (indexed && docValued) {
                    onField.accept(new FloatField(name, value.floatValue(), Store.NO));
                } else {
                    if (indexed) {
                        onField.accept(new FloatPoint(name, value.floatValue()));
                    }
                    if (docValued) {
                        onField.accept(new SortedNumericDocValuesField(name,
                            NumericUtils.floatToSortableInt(value.floatValue())));
                    }
                }
                if (stored) {
                    onField.accept(new StoredField(name, value.floatValue()));
                }
            }
        },
        DOUBLE("double") {
            @Override
            public Double parse(Object value, boolean coerce) {
                return objectToDouble(value);
            }

            @Override
            public Double parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.doubleValue(coerce);
            }

            @Override
            public void createFields(Consumer<IndexableField> onField,
                                     String name,
                                     Number value,
                                     boolean indexed,
                                     boolean docValued,
                                     boolean stored) {
                if (indexed) {
                    onField.accept(new DoublePoint(name, value.doubleValue()));
                }
                if (docValued) {
                    onField.accept(new SortedNumericDocValuesField(name,
                        NumericUtils.doubleToSortableLong(value.doubleValue())));
                }
                if (stored) {
                    onField.accept(new StoredField(name, value.doubleValue()));
                }
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

                if (value instanceof Number number) {
                    return number.byteValue();
                }

                return (byte) doubleValue;
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
            public void createFields(Consumer<IndexableField> onField,
                                     String name,
                                     Number value,
                                     boolean indexed,
                                     boolean docValued,
                                     boolean stored) {
                INTEGER.createFields(onField, name, value, indexed, docValued, stored);
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

                if (value instanceof Number number) {
                    return number.shortValue();
                }

                return (short) doubleValue;
            }

            @Override
            public Short parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.shortValue(coerce);
            }

            @Override
            public void createFields(Consumer<IndexableField> onField,
                                     String name,
                                     Number value,
                                     boolean indexed,
                                     boolean docValued,
                                     boolean stored) {
                INTEGER.createFields(onField, name, value, indexed, docValued, stored);
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

                if (value instanceof Number number) {
                    return number.intValue();
                }

                return (int) doubleValue;
            }

            @Override
            public Integer parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.intValue(coerce);
            }

            @Override
            public void createFields(Consumer<IndexableField> onField,
                                     String name,
                                     Number value,
                                     boolean indexed,
                                     boolean docValued,
                                     boolean stored) {
                if (indexed) {
                    onField.accept(new IntPoint(name, value.intValue()));
                }
                if (docValued) {
                    onField.accept(new SortedNumericDocValuesField(name, value.intValue()));
                }
                if (stored) {
                    onField.accept(new StoredField(name, value.intValue()));
                }
            }
        },
        LONG("long") {
            @Override
            public Long parse(Object value, boolean coerce) {
                if (value instanceof Long l) {
                    return l;
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
                String stringValue = (value instanceof BytesRef bytesRef) ? bytesRef.utf8ToString() : value.toString();
                return Numbers.toLong(stringValue, coerce);
            }

            @Override
            public Long parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.longValue(coerce);
            }

            @Override
            public void createFields(Consumer<IndexableField> onField,
                                     String name,
                                     Number value,
                                     boolean indexed,
                                     boolean docValued,
                                     boolean stored) {
                if (indexed) {
                    onField.accept(new LongPoint(name, value.longValue()));
                }
                if (docValued) {
                    onField.accept(new SortedNumericDocValuesField(name, value.longValue()));
                }
                if (stored) {
                    onField.accept(new StoredField(name, value.longValue()));
                }
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

        public abstract void createFields(Consumer<IndexableField> onField,
                                          String name,
                                          Number value,
                                          boolean indexed,
                                          boolean docValued,
                                          boolean stored);

        /**
         * Converts an Object to a double by checking it against known types first
         */
        private static double objectToDouble(Object value) {
            double doubleValue;

            if (value instanceof Number number) {
                doubleValue = number.doubleValue();
            } else if (value instanceof BytesRef bytesRef) {
                doubleValue = Double.parseDouble(bytesRef.utf8ToString());
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
            long columnOID,
            boolean isDropped,
            @Nullable String defaultExpression,
            FieldType fieldType,
            MappedFieldType mappedFieldType,
            CopyTo copyTo) {
        super(simpleName, position, columnOID, isDropped, defaultExpression, fieldType, mappedFieldType, copyTo);
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
    protected void parseCreateField(ParseContext context, Consumer<IndexableField> onField) throws IOException {
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
        fieldType().type.createFields(
            onField,
            fieldType().name(),
            numericValue,
            fieldType().isSearchable(),
            docValued,
            stored
        );
        if (docValued == false && (stored || fieldType().isSearchable())) {
            createFieldNamesField(context, onField);
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
