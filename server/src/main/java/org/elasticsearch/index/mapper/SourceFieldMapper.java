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

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class SourceFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_source";
    public static final String RECOVERY_SOURCE_NAME = "_recovery_source";

    public static final String CONTENT_TYPE = "_source";

    public static class Defaults {
        public static final String NAME = SourceFieldMapper.NAME;

        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE); // not indexed
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }

    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder> {


        public Builder() {
            super(Defaults.NAME, new FieldType(Defaults.FIELD_TYPE));
        }

        @Override
        public SourceFieldMapper build(BuilderContext context) {
            return new SourceFieldMapper(context.indexSettings());
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {

        @Override
        public MetadataFieldMapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            return new Builder();
        }

        @Override
        public MetadataFieldMapper getDefault(MappedFieldType fieldType, ParserContext context) {
            final Settings indexSettings = context.mapperService().getIndexSettings().getSettings();
            return new SourceFieldMapper(indexSettings);
        }
    }

    static final class SourceFieldType extends MappedFieldType {

        public static final SourceFieldType INSTANCE = new SourceFieldType();

        private SourceFieldType() {
            super(NAME, false, false);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

    }


    private SourceFieldMapper(Settings indexSettings) {
        super(Defaults.FIELD_TYPE, SourceFieldType.INSTANCE, indexSettings);
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        // nothing to do here, we will call it in pre parse
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        BytesReference originalSource = context.sourceToParse().source();
        BytesReference source = originalSource;
        if (fieldType.stored() && source != null) {
            // Percolate and tv APIs may not set the source and that is ok, because these APIs will not index any data
            BytesRef ref = source.toBytesRef();
            fields.add(new StoredField(fieldType().name(), ref.bytes, ref.offset, ref.length));
        } else {
            source = null;
        }

        if (originalSource != null && source != originalSource && context.indexSettings().isSoftDeleteEnabled()) {
            // if we omitted source or modified it we add the _recovery_source to ensure we
            // have it for ops based recovery
            BytesRef ref = originalSource.toBytesRef();
            fields.add(new StoredField(RECOVERY_SOURCE_NAME, ref.bytes, ref.offset, ref.length));
            fields.add(new NumericDocValuesField(RECOVERY_SOURCE_NAME, 1));
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
    }
}
