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

import static org.elasticsearch.index.mapper.TypeParsers.parseField;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.FieldType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.jetbrains.annotations.Nullable;

/**
 * Field Mapper for geo_point types.
 *
 * Uses lucene 6 LatLonPoint encoding
 */
public class GeoPointFieldMapper extends FieldMapper implements ArrayValueMapperParser {
    public static final String CONTENT_TYPE = "geo_point";
    public static final FieldType FIELD_TYPE = new FieldType();

    static {
        FIELD_TYPE.setStored(false);
        FIELD_TYPE.setTokenized(false);
        FIELD_TYPE.setDimensions(2, Integer.BYTES);
        FIELD_TYPE.freeze();
    }

    public static class Builder extends FieldMapper.Builder {

        public Builder(String name) {
            super(name, FIELD_TYPE);
            hasDocValues = true;
        }


        @Override
        public GeoPointFieldMapper build(BuilderContext context) {
            var ft = new GeoPointFieldType(buildFullName(context), indexed, hasDocValues);
            var mapper = new GeoPointFieldMapper(
                name,
                position,
                columnOID,
                isDropped,
                defaultExpression,
                fieldType,
                ft,
                copyTo);
            context.putPositionInfo(mapper, position);
            return mapper;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name,
                                       Map<String, Object> node,
                                       ParserContext parserContext) throws MapperParsingException {
            Builder builder = new GeoPointFieldMapper.Builder(name);
            parseField(builder, name, node);
            return builder;
        }
    }

    public GeoPointFieldMapper(String simpleName,
                               int position,
                               long columnOID,
                               boolean isDropped,
                               @Nullable String defaultExpression,
                               FieldType fieldType,
                               MappedFieldType defaultFieldType,
                               CopyTo copyTo) {
        super(simpleName, position, columnOID, isDropped, defaultExpression, fieldType, defaultFieldType, copyTo);
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public static class GeoPointFieldType extends MappedFieldType {

        public GeoPointFieldType(String name, boolean indexed, boolean hasDocValues) {
            super(name, indexed, hasDocValues);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults) throws IOException {
        super.doXContentBody(builder, includeDefaults);
    }
}
