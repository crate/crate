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

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.function.BiFunction;

public class MultiValuesSourceFieldConfig implements Writeable, ToXContentFragment {
    private String fieldName;
    private Object missing;
    private Script script;
    private DateTimeZone timeZone;

    private static final String NAME = "field_config";

    public static final BiFunction<Boolean, Boolean, ObjectParser<MultiValuesSourceFieldConfig.Builder, Void>> PARSER
        = (scriptable, timezoneAware) -> {

        ObjectParser<MultiValuesSourceFieldConfig.Builder, Void> parser
            = new ObjectParser<>(MultiValuesSourceFieldConfig.NAME, MultiValuesSourceFieldConfig.Builder::new);

        parser.declareString(MultiValuesSourceFieldConfig.Builder::setFieldName, ParseField.CommonFields.FIELD);
        parser.declareField(MultiValuesSourceFieldConfig.Builder::setMissing, XContentParser::objectText,
            ParseField.CommonFields.MISSING, ObjectParser.ValueType.VALUE);

        if (scriptable) {
            parser.declareField(MultiValuesSourceFieldConfig.Builder::setScript,
                (p, context) -> Script.parse(p),
                Script.SCRIPT_PARSE_FIELD, ObjectParser.ValueType.OBJECT_OR_STRING);
        }

        if (timezoneAware) {
            parser.declareField(MultiValuesSourceFieldConfig.Builder::setTimeZone, p -> {
                if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return DateTimeZone.forID(p.text());
                } else {
                    return DateTimeZone.forOffsetHours(p.intValue());
                }
            }, ParseField.CommonFields.TIME_ZONE, ObjectParser.ValueType.LONG);
        }
        return parser;
    };

    private MultiValuesSourceFieldConfig(String fieldName, Object missing, Script script, DateTimeZone timeZone) {
        this.fieldName = fieldName;
        this.missing = missing;
        this.script = script;
        this.timeZone = timeZone;
    }

    public MultiValuesSourceFieldConfig(StreamInput in) throws IOException {
        this.fieldName = in.readString();
        this.missing = in.readGenericValue();
        this.script = in.readOptionalWriteable(Script::new);
        this.timeZone = in.readOptionalTimeZone();
    }

    public Object getMissing() {
        return missing;
    }

    public Script getScript() {
        return script;
    }

    public DateTimeZone getTimeZone() {
        return timeZone;
    }

    public String getFieldName() {
        return fieldName;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeGenericValue(missing);
        out.writeOptionalWriteable(script);
        out.writeOptionalTimeZone(timeZone);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (missing != null) {
            builder.field(ParseField.CommonFields.MISSING.getPreferredName(), missing);
        }
        if (script != null) {
            builder.field(Script.SCRIPT_PARSE_FIELD.getPreferredName(), script);
        }
        if (fieldName != null) {
            builder.field(ParseField.CommonFields.FIELD.getPreferredName(), fieldName);
        }
        if (timeZone != null) {
            builder.field(ParseField.CommonFields.TIME_ZONE.getPreferredName(), timeZone);
        }
        return builder;
    }

    public static class Builder {
        private String fieldName;
        private Object missing = null;
        private Script script = null;
        private DateTimeZone timeZone = null;

        public String getFieldName() {
            return fieldName;
        }

        public Builder setFieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Object getMissing() {
            return missing;
        }

        public Builder setMissing(Object missing) {
            this.missing = missing;
            return this;
        }

        public Script getScript() {
            return script;
        }

        public Builder setScript(Script script) {
            this.script = script;
            return this;
        }

        public DateTimeZone getTimeZone() {
            return timeZone;
        }

        public Builder setTimeZone(DateTimeZone timeZone) {
            this.timeZone = timeZone;
            return this;
        }

        public MultiValuesSourceFieldConfig build() {
            if (Strings.isNullOrEmpty(fieldName) && script == null) {
                throw new IllegalArgumentException("[" +  ParseField.CommonFields.FIELD.getPreferredName()
                    + "] and [" + Script.SCRIPT_PARSE_FIELD.getPreferredName() + "] cannot both be null.  " +
                    "Please specify one or the other.");
            }

            if (Strings.isNullOrEmpty(fieldName) == false && script != null) {
                throw new IllegalArgumentException("[" +  ParseField.CommonFields.FIELD.getPreferredName()
                    + "] and [" + Script.SCRIPT_PARSE_FIELD.getPreferredName() + "] cannot both be configured.  " +
                    "Please specify one or the other.");
            }

            return new MultiValuesSourceFieldConfig(fieldName, missing, script, timeZone);
        }
    }
}
