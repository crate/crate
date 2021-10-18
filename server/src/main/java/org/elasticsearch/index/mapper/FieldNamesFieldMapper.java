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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.settings.Settings;
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

    static class Builder extends MetadataFieldMapper.Builder<Builder> {
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
            return new FieldNamesFieldMapper(fieldType, fieldNamesFieldType, context.indexSettings());
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            return new Builder(parserContext.mapperService().fullName(NAME));
        }

        @Override
        public MetadataFieldMapper getDefault(MappedFieldType fieldType, ParserContext context) {
            final Settings indexSettings = context.mapperService().getIndexSettings().getSettings();
            if (fieldType != null) {
                return new FieldNamesFieldMapper(Defaults.FIELD_TYPE, fieldType, indexSettings);
            } else {
                return parse(NAME, Collections.emptyMap(), context)
                        .build(new BuilderContext(indexSettings, new ContentPath(1)));
            }
        }
    }

    public static final class FieldNamesFieldType extends TermBasedFieldType {

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

    private FieldNamesFieldMapper(FieldType fieldType, MappedFieldType mappedFieldType, Settings indexSettings) {
        super(fieldType, mappedFieldType, indexSettings);
    }

    @Override
    public FieldNamesFieldType fieldType() {
        return (FieldNamesFieldType) super.fieldType();
    }

    @Override
    public void preParse(ParseContext context) {
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        // Adding values to the _field_names field is handled by the mappers for each field type
    }

    static Iterable<String> extractFieldNames(final String fullPath) {
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return new Iterator<String>() {

                    int endIndex = nextEndIndex(0);

                    private int nextEndIndex(int index) {
                        while (index < fullPath.length() && fullPath.charAt(index) != '.') {
                            index += 1;
                        }
                        return index;
                    }

                    @Override
                    public boolean hasNext() {
                        return endIndex <= fullPath.length();
                    }

                    @Override
                    public String next() {
                        final String result = fullPath.substring(0, endIndex);
                        endIndex = nextEndIndex(endIndex + 1);
                        return result;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }

                };
            }
        };
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        if (fieldType().isEnabled() == false) {
            return;
        }
        for (ParseContext.Document document : context) {
            final List<String> paths = new ArrayList<>(document.getFields().size());
            String previousPath = ""; // used as a sentinel - field names can't be empty
            for (IndexableField field : document.getFields()) {
                final String path = field.name();
                if (path.equals(previousPath)) {
                    // Sometimes mappers create multiple Lucene fields, eg. one for indexing,
                    // one for doc values and one for storing. Deduplicating is not required
                    // for correctness but this simple check helps save utf-8 conversions and
                    // gives Lucene fewer values to deal with.
                    continue;
                }
                paths.add(path);
                previousPath = path;
            }
            for (String path : paths) {
                for (String fieldName : extractFieldNames(path)) {
                    if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
                        document.add(new Field(fieldType().name(), fieldName, fieldType));
                    }
                }
            }
        }
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
