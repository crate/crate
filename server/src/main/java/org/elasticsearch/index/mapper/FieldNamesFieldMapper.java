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
import java.util.Collections;
import java.util.Map;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * A mapper that indexes the field names of a document under <code>_field_names</code>. This mapper is typically useful in order
 * to have fast <code>exists</code> and <code>missing</code> queries/filters.
 *
 * Added in Elasticsearch 1.3.
 */
public class FieldNamesFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_field_names";

    public static final String CONTENT_TYPE = "_field_names";

    public static class Defaults {
        public static final String NAME = FieldNamesFieldMapper.NAME;

        public static final boolean ENABLED = true;
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    static class Builder extends MetadataFieldMapper.Builder {
        private boolean enabled = Defaults.ENABLED;

        public Builder(MappedFieldType existing) {
            super(Defaults.NAME, Defaults.FIELD_TYPE);
        }

        Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        @Override
        public FieldNamesFieldMapper build(BuilderContext context) {
            FieldNamesFieldType fieldNamesFieldType = new FieldNamesFieldType();
            fieldNamesFieldType.setEnabled(enabled);
            return new FieldNamesFieldMapper(fieldType, fieldNamesFieldType);
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            return new Builder(parserContext.mapperService().fieldType(NAME));
        }

        @Override
        public MetadataFieldMapper getDefault(ParserContext context) {
            return parse(NAME, Collections.emptyMap(), context)
                    .build(new BuilderContext(new ContentPath(1)));
        }
    }

    public static final class FieldNamesFieldType extends MappedFieldType {

        private boolean enabled = Defaults.ENABLED;

        public FieldNamesFieldType() {
            super(Defaults.NAME, true, false);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public boolean isEnabled() {
            return enabled;
        }

    }

    private FieldNamesFieldMapper(FieldType fieldType, MappedFieldType mappedFieldType) {
        super(fieldType, mappedFieldType);
    }

    @Override
    public FieldNamesFieldType fieldType() {
        return (FieldNamesFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        if (includeDefaults == false && fieldType().isEnabled() == Defaults.ENABLED) {
            return builder;
        }

        builder.startObject(NAME);
        if (includeDefaults || fieldType().isEnabled() != Defaults.ENABLED) {
            builder.field("enabled", fieldType().isEnabled());
        }

        builder.endObject();
        return builder;
    }

}
