/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.elasticsearch.index.mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

/**
 * fieldmapper for encoding and handling of primitive arrays (non-object) explicitly
 * <p>
 * handler for type "array".
 * <p>
 * accepts mappings like:
 * <pre>
 * "array_field": {
 *      "type": "array",
 *      "inner": {
 *          "type": "boolean",
 *          "null_value": true
 *      }
 * }
 *
 * <pre>
 * This would be parsed as a array of booleans.
 * This field now only accepts arrays, no single values.
 * So inserting a document like:
 * <pre>
 * {
 *      "array_field": true
 * }
 * <pre>
 *
 * will fail, while a document like:
 * <pre>
 * {
 *      "array_field": [true]
 * }
 * </pre>
 * will pass.
 */
public class ArrayMapper extends FieldMapper implements ArrayValueMapperParser {

    public static final String CONTENT_TYPE = "array";
    public static final String INNER_TYPE = "inner";
    private Mapper innerMapper;

    ArrayMapper(String simpleName,
                int position,
                @Nullable String defaultExpression,
                FieldType fieldType,
                MappedFieldType defaultFieldType,
                Settings indexSettings,
                Mapper innerMapper) {
        super(simpleName, position, defaultExpression, fieldType, defaultFieldType, indexSettings, CopyTo.empty());
        this.innerMapper = innerMapper;
    }

    private static FieldType getFieldType(Mapper.Builder<?> builder) {
        if (builder instanceof FieldMapper.Builder<?> fieldMapperBuilder) {
            return fieldMapperBuilder.fieldType;
        }
        throw new IllegalArgumentException("expected a FieldMapper.Builder");
    }

    public static class Builder extends FieldMapper.Builder<Builder> {

        private final Mapper.Builder<?> innerBuilder;

        public Builder(String name, Mapper.Builder<?> innerBuilder) {
            super(name, getFieldType(innerBuilder));
            this.innerBuilder = innerBuilder;
        }

        @Override
        public ArrayMapper build(BuilderContext context) {
            Mapper innerMapper = innerBuilder.build(context);
            MappedFieldType mappedFieldType = null;
            if (innerMapper instanceof FieldMapper fieldMapper) {
                mappedFieldType = fieldMapper.fieldType();
            }
            return new ArrayMapper(
                name,
                position,
                defaultExpression,
                fieldType,
                mappedFieldType,
                context.indexSettings(),
                innerMapper
            );
        }
    }

    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return ArrayMapper.toXContent(builder, params, innerMapper, simpleName(), contentType());
    }

    static XContentBuilder toXContent(XContentBuilder builder,
                                      Params params,
                                      Mapper innerMapper,
                                      String name,
                                      String contentType) throws IOException {
        /*
         * array mapping should look like:
         *
         * "fieldName": {
         *      "type": "array":
         *      "inner": {
         *          "type": "string"
         *          ...
         *      }
         * }
         *
         *
         * Use the innerMapper to generate the mapping for the inner type which will look like:
         *
         * "fieldName": {
         *      "type": "string",
         *      ...
         * }
         *
         * and then parse the contents of the object to set it into the "inner" field of the outer array type.
         */
        XContentBuilder innerBuilder = new XContentBuilder(builder.contentType().xContent(), new BytesStreamOutput(0));
        innerBuilder.startObject();
        innerBuilder = innerMapper.toXContent(innerBuilder, params);
        innerBuilder.endObject();
        innerBuilder.close();
        XContentParser parser = builder.contentType().xContent().createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            BytesReference.toBytes(BytesReference.bytes(innerBuilder)));

        //noinspection StatementWithEmptyBody
        while ((parser.nextToken() != XContentParser.Token.START_OBJECT)) {
            // consume tokens until start of object
        }

        //noinspection unchecked
        Map<String, Object> innerMap = (Map<String, Object>) parser.mapOrdered().get(innerMapper.simpleName());

        assert innerMap != null : "innerMap was null";

        builder.startObject(name);
        builder.field("type", contentType);
        builder.field(INNER_TYPE, innerMap);
        return builder.endObject();
    }

    public Iterator<Mapper> iterator() {
        return innerMapper.iterator();
    }


    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        if (other instanceof ArrayMapper arrayMapper) {
            innerMapper.merge(arrayMapper.innerMapper);
        } else {
            innerMapper.merge(other);
        }
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        XContentParser parser = context.parser();
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_NULL) {
            parseInner(context);
            return;
        }
        if (token == XContentParser.Token.START_ARRAY) {
            token = parser.nextToken();
        }
        Mapper newInnerMapper = innerMapper;
        while (token != XContentParser.Token.END_ARRAY) {
            // we only get here for non-empty arrays
            parseInner(context);
            token = parser.nextToken();
        }
        if (newInnerMapper == innerMapper) {
            return;
        }
        innerMapper = newInnerMapper;
    }

    private void parseInner(ParseContext context) throws IOException {
        assert innerMapper instanceof FieldMapper : "InnerMapper must be a FieldMapper";
        ((FieldMapper) innerMapper).parse(context);
        if (copyTo() != null) {
            DocumentParser.parseCopyFields(context, copyTo().copyToFields());
        }
    }

    @Override
    public CopyTo copyTo() {
        if (innerMapper instanceof FieldMapper) {
            return ((FieldMapper) innerMapper).copyTo();
        }
        return null;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        // parseCreateField is called in the original FieldMapper parse method.
        // Since parse is overwritten parseCreateField is never called
        throw new UnsupportedOperationException("parseCreateField not supported for " +
                                                ArrayMapper.class.getSimpleName());
    }

    @Override
    public int maxColumnPosition() {
        return Math.max(super.maxColumnPosition(), innerMapper.maxColumnPosition());
    }
}
