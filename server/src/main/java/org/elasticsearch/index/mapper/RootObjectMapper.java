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

import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import javax.annotation.Nullable;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.TypeParsers.parseDateTimeFormatter;

public class RootObjectMapper extends ObjectMapper {

    public static class Defaults {
        public static final FormatDateTimeFormatter[] DYNAMIC_DATE_TIME_FORMATTERS =
            new FormatDateTimeFormatter[] {
                DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
                Joda.getStrictStandardDateFormatter()
            };
    }

    public static class Builder extends ObjectMapper.Builder<Builder> {

        protected Explicit<FormatDateTimeFormatter[]> dynamicDateTimeFormatters = new Explicit<>(Defaults.DYNAMIC_DATE_TIME_FORMATTERS, false);

        public Builder(String name) {
            super(name);
            this.builder = this;
        }

        public Builder dynamicDateTimeFormatter(Collection<FormatDateTimeFormatter> dateTimeFormatters) {
            this.dynamicDateTimeFormatters = new Explicit<>(dateTimeFormatters.toArray(new FormatDateTimeFormatter[0]), true);
            return this;
        }

        @Override
        public RootObjectMapper build(BuilderContext context) {
            return (RootObjectMapper) super.build(context);
        }

        @Override
        protected ObjectMapper createMapper(String name, Integer position, String fullPath, Dynamic dynamic,
                Map<String, Mapper> mappers, @Nullable Settings settings) {
            return new RootObjectMapper(
                name,
                dynamic,
                mappers,
                dynamicDateTimeFormatters,
                settings
            );
        }
    }

    public static class TypeParser extends ObjectMapper.TypeParser {

        @Override
        @SuppressWarnings("rawtypes")
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {

            RootObjectMapper.Builder builder = new Builder(name);
            Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (parseObjectOrDocumentTypeProperties(fieldName, fieldNode, parserContext, builder)
                        || processField(builder, fieldName, fieldNode, parserContext.indexVersionCreated())) {
                    iterator.remove();
                }
            }
            return builder;
        }

        @SuppressWarnings("unchecked")
        protected boolean processField(RootObjectMapper.Builder builder, String fieldName, Object fieldNode,
                                       Version indexVersionCreated) {
            if (fieldName.equals("date_formats") || fieldName.equals("dynamic_date_formats")) {
                if (fieldNode instanceof List) {
                    List<FormatDateTimeFormatter> formatters = new ArrayList<>();
                    for (Object formatter : (List<?>) fieldNode) {
                        if (formatter.toString().startsWith("epoch_")) {
                            throw new MapperParsingException(
                                "Epoch [" + formatter + "] is not supported as dynamic date format");
                        }
                        formatters.add(parseDateTimeFormatter(formatter));
                    }
                    builder.dynamicDateTimeFormatter(formatters);
                } else if ("none".equals(fieldNode.toString())) {
                    builder.dynamicDateTimeFormatter(Collections.emptyList());
                } else {
                    builder.dynamicDateTimeFormatter(Collections.singleton(parseDateTimeFormatter(fieldNode)));
                }
                return true;
            }
            return false;
        }
    }

    private Explicit<FormatDateTimeFormatter[]> dynamicDateTimeFormatters;

    RootObjectMapper(String name,
                     Dynamic dynamic,
                     Map<String, Mapper> mappers,
                     Explicit<FormatDateTimeFormatter[]> dynamicDateTimeFormatters,
                     Settings settings) {
        super(name, null, name, dynamic, mappers, settings);
        this.dynamicDateTimeFormatters = dynamicDateTimeFormatters;
    }

    @Override
    public ObjectMapper mappingUpdate(Mapper mapper) {
        RootObjectMapper update = (RootObjectMapper) super.mappingUpdate(mapper);
        // for dynamic updates, no need to carry root-specific options, we just
        // set everything to they implicit default value so that they are not
        // applied at merge time
        update.dynamicDateTimeFormatters = new Explicit<FormatDateTimeFormatter[]>(Defaults.DYNAMIC_DATE_TIME_FORMATTERS, false);
        return update;
    }

    public FormatDateTimeFormatter[] dynamicDateTimeFormatters() {
        return dynamicDateTimeFormatters.value();
    }

    @Override
    public RootObjectMapper merge(Mapper mergeWith) {
        return (RootObjectMapper) super.merge(mergeWith);
    }

    @Override
    protected void doMerge(ObjectMapper mergeWith) {
        super.doMerge(mergeWith);
        RootObjectMapper mergeWithObject = (RootObjectMapper) mergeWith;
        if (mergeWithObject.dynamicDateTimeFormatters.explicit()) {
            this.dynamicDateTimeFormatters = mergeWithObject.dynamicDateTimeFormatters;
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        final boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        if (dynamicDateTimeFormatters.explicit() || includeDefaults) {
            builder.startArray("dynamic_date_formats");
            for (FormatDateTimeFormatter dateTimeFormatter : dynamicDateTimeFormatters.value()) {
                builder.value(dateTimeFormatter.format());
            }
            builder.endArray();
        }
    }
}
