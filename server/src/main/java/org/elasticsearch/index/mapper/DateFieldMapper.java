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

import static io.crate.server.xcontent.XContentMapValues.nodeBooleanValue;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.jetbrains.annotations.Nullable;

import io.crate.common.time.IsoLocale;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.xcontent.XContentBuilder;

/** A {@link FieldMapper} for ip addresses. */
public class DateFieldMapper extends FieldMapper {

    private static final String DEFAULT_FORMAT_PATTERN = "strict_date_optional_time||epoch_millis";
    public static final String CONTENT_TYPE = "date";
    public static final FormatDateTimeFormatter DEFAULT_DATE_TIME_FORMATTER = Joda.forPattern(DEFAULT_FORMAT_PATTERN);

    public static final class Defaults {

        private Defaults() {}

        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(true);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setStoreTermVectors(false);
            FIELD_TYPE.setOmitNorms(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        }
    }

    public static class Builder extends FieldMapper.Builder {

        private Explicit<String> format = new Explicit<>(DEFAULT_FORMAT_PATTERN, false);
        private Boolean ignoreTimezone;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
        }

        public void ignoreTimezone(boolean ignoreTimezone) {
            this.ignoreTimezone = ignoreTimezone;
        }

        public void format(String format) {
            this.format = new Explicit<>(format, true);
        }

        protected DateFieldType setupFieldType(BuilderContext context) {
            String pattern = this.format.value();
            var formatter = Joda.forPattern(pattern);
            return new DateFieldType(buildFullName(context), indexed, hasDocValues, formatter);
        }

        @Override
        public DateFieldMapper build(BuilderContext context) {
            DateFieldType ft = setupFieldType(context);
            var mapper = new DateFieldMapper(
                name,
                position,
                columnOID,
                isDropped,
                defaultExpression,
                fieldType,
                ft,
                ignoreTimezone,
                copyTo);
            context.putPositionInfo(mapper, position);
            return mapper;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        public TypeParser() {
        }

        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name);
            TypeParsers.parseField(builder, name, node);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals("format")) {
                    builder.format(propNode.toString());
                    iterator.remove();
                } else if (propName.equals("ignore_timezone")) {
                    builder.ignoreTimezone(nodeBooleanValue(propNode, "ignore_timezone"));
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class DateFieldType extends MappedFieldType {
        private final FormatDateTimeFormatter dateTimeFormatter;

        DateFieldType(String name, boolean isSearchable, boolean hasDocValues, FormatDateTimeFormatter formatter) {
            super(name, isSearchable, hasDocValues);
            this.dateTimeFormatter = formatter;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public FormatDateTimeFormatter dateTimeFormatter() {
            return dateTimeFormatter;
        }

        long parse(String value) {
            return dateTimeFormatter().parser().parseMillis(value);
        }

    }

    private final Boolean ignoreTimezone;

    private DateFieldMapper(
            String simpleName,
            int position,
            long columnOID,
            boolean isDropped,
            @Nullable String defaultExpression,
            FieldType fieldType,
            MappedFieldType mappedFieldType,
            Boolean ignoreTimezone,
            CopyTo copyTo) {
        super(simpleName, position, columnOID, isDropped, defaultExpression, fieldType, mappedFieldType, copyTo);
        this.ignoreTimezone = ignoreTimezone;
    }

    @Override
    public DateFieldType fieldType() {
        return (DateFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return fieldType().typeName();
    }

    @Override
    protected DateFieldMapper clone() {
        return (DateFieldMapper) super.clone();
    }

    @Override
    protected void parseCreateField(ParseContext context, Consumer<IndexableField> onField) throws IOException {
        String dateAsString = context.parser().textOrNull();

        long timestamp;
        if (dateAsString == null) {
            return;
        } else {
            timestamp = fieldType().parse(dateAsString);
        }

        if (mappedFieldType.isSearchable() && mappedFieldType.hasDocValues()) {
            onField.accept(new LongField(fieldType().name(), timestamp, Field.Store.NO));
        } else {
            if (mappedFieldType.isSearchable()) {
                onField.accept(new LongPoint(fieldType().name(), timestamp));
            }
            if (mappedFieldType.hasDocValues()) {
                onField.accept(new SortedNumericDocValuesField(fieldType().name(), timestamp));
            } else if (fieldType.stored() || mappedFieldType.isSearchable()) {
                createFieldNamesField(context, onField);
            }
        }
        if (fieldType.stored()) {
            onField.accept(new StoredField(fieldType().name(), timestamp));
        }
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        final DateFieldMapper d = (DateFieldMapper) other;
        if (Objects.equals(fieldType().dateTimeFormatter().format(), d.fieldType().dateTimeFormatter().format()) == false) {
            conflicts.add("mapper [" + name() + "] has different [format] values");
        }
        if (Objects.equals(fieldType().dateTimeFormatter().locale(), d.fieldType().dateTimeFormatter().locale()) == false) {
            conflicts.add("mapper [" + name() + "] has different [locale] values");
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults) throws IOException {
        super.doXContentBody(builder, includeDefaults);

        if (includeDefaults
                || fieldType().dateTimeFormatter().format().equals(DEFAULT_DATE_TIME_FORMATTER.format()) == false) {
            builder.field("format", fieldType().dateTimeFormatter().format());
        }

        if (includeDefaults || ignoreTimezone != null) {
            builder.field("ignore_timezone", ignoreTimezone);
        }

        if (includeDefaults
                || fieldType().dateTimeFormatter().locale() != IsoLocale.ROOT) {
            builder.field("locale", fieldType().dateTimeFormatter().locale());
        }
    }
}
