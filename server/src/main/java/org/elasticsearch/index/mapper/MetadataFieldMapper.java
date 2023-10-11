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

import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;


/**
 * A mapper for a builtin field containing metadata about a document.
 */
public abstract class MetadataFieldMapper extends FieldMapper {

    public interface TypeParser extends Mapper.TypeParser {

        @Override
        MetadataFieldMapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException;

        /**
         * Get the default {@link MetadataFieldMapper} to use, if nothing had to be parsed.
         * @param parserContext context that may be useful to build the field like analyzers
         */
        MetadataFieldMapper getDefault(ParserContext parserContext);
    }

    public abstract static class Builder<T extends Builder<T>> extends FieldMapper.Builder<T> {
        public Builder(String name, FieldType fieldType) {
            super(name, fieldType);
        }

        @Override
        public T index(boolean index) {
            if (index == false) {
                throw new IllegalArgumentException("Metadata fields must be indexed");
            }
            return builder;
        }

        public abstract MetadataFieldMapper build(BuilderContext context);
    }

    protected MetadataFieldMapper(FieldType fieldType, MappedFieldType mappedFieldType) {
        super(
            mappedFieldType.name(),
            NOT_TO_BE_POSITIONED,
            COLUMN_OID_UNASSIGNED,
            false,
            null,
            fieldType,
            mappedFieldType,
            CopyTo.empty()
        );
    }

    /**
     * Called before {@link FieldMapper#parse(ParseContext)} on the {@link RootObjectMapper}.
     */
    public abstract void preParse(ParseContext context) throws IOException;

    /**
     * Called after {@link FieldMapper#parse(ParseContext)} on the {@link RootObjectMapper}.
     */
    public void postParse(ParseContext context) throws IOException {
        // do nothing
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
    }
}
