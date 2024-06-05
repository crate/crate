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
import java.util.Objects;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.jetbrains.annotations.Nullable;

import io.crate.common.time.IsoLocale;

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
