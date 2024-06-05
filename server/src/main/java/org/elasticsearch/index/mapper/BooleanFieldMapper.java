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

import java.util.List;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.Nullable;

/**
 * A field mapper for boolean fields.
 */
public class BooleanFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "boolean";

    public static final class Defaults {

        private Defaults() {}

        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.freeze();
        }
    }

    public static class Values {
        public static final BytesRef TRUE = new BytesRef("T");
        public static final BytesRef FALSE = new BytesRef("F");
    }

    public static class Builder extends FieldMapper.Builder {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
        }

        @Override
        public BooleanFieldMapper build(BuilderContext context) {
            var mapper = new BooleanFieldMapper(
                name,
                position,
                columnOID,
                isDropped,
                defaultExpression,
                fieldType,
                new BooleanFieldType(buildFullName(context), indexed, hasDocValues),
                copyTo);
            context.putPositionInfo(mapper, position);
            return mapper;
        }
    }

    public static final class BooleanFieldType extends MappedFieldType {

        public BooleanFieldType(String name, boolean isSearchable, boolean hasDocValues) {
            super(name, isSearchable, hasDocValues);
        }

        public BooleanFieldType(String name) {
            this(name, true, true);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }
    }

    protected BooleanFieldMapper(String simpleName,
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
    public BooleanFieldType fieldType() {
        return (BooleanFieldType) super.fieldType();
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {

    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
