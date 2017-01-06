/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package org.elasticsearch.index.mapper;

import com.google.common.base.Throwables;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.array.DynamicArrayFieldMapperBuilderFactory;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * fieldmapper for encoding and handling arrays explicitly
 * <p>
 * handler for type "array".
 * <p>
 * accepts mappings like:
 * <p>
 * "array_field": {
 * "type": "array",
 * "inner": {
 * "type": "boolean",
 * "null_value": true
 * }
 * }
 * <p>
 * This would be parsed as a array of booleans.
 * This field now only accepts arrays, no single values.
 * So inserting a document like:
 * <p>
 * {
 * "array_field": true
 * }
 * <p>
 * will fail, while a document like:
 * <p>
 * {
 * "array_field": [true]
 * }
 * <p>
 * will pass.
 */
public class ArrayMapper extends FieldMapper implements ArrayValueMapperParser {

    public static final String CONTENT_TYPE = "array";

    private static final String INNER_TYPE = "inner";
    private Mapper innerMapper;

    ArrayMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                          Settings indexSettings, MultiFields multiFields, CopyTo copyTo, Mapper innerMapper) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.innerMapper = innerMapper;
    }

    public static class BuilderFactory implements DynamicArrayFieldMapperBuilderFactory {

        public Mapper create(String name, ObjectMapper parentMapper, ParseContext context) {
            BuilderContext builderContext = new BuilderContext(context.indexSettings(), context.path());
            try {
                Mapper.Builder<?, ?> innerBuilder = detectInnerMapper(context, name, context.parser());
                if (innerBuilder == null) {
                    return null;
                }
                Mapper mapper = innerBuilder.build(builderContext);
                DocumentParser.parseObjectOrField(context, mapper);
                MappedFieldType mappedFieldType = newArrayFieldType(innerBuilder);
                mappedFieldType.setName(name);
                return new ArrayMapper(
                    name,
                    mappedFieldType,
                    mappedFieldType.clone(),
                    context.indexSettings(),
                    MultiFields.empty(),
                    null,
                    mapper);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static MappedFieldType newArrayFieldType(Mapper.Builder innerBuilder) {
        if (innerBuilder instanceof FieldMapper.Builder) {
            return new ArrayFieldType(((FieldMapper.Builder) innerBuilder).fieldType());
        }
        if (innerBuilder instanceof ObjectMapper.Builder) {
            return new ObjectArrayFieldType();
        }
        throw new IllegalArgumentException("expected a FieldMapper.Builder or ObjectMapper.Builder");
    }

    static class ObjectArrayFieldType extends MappedFieldType implements Cloneable {

        ObjectArrayFieldType() {
        }

        ObjectArrayFieldType(MappedFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new ObjectArrayFieldType(this);
        }

        @Override
        public String typeName() {
            return ArrayMapper.CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, @Nullable QueryShardContext context) {
            // FIXME: return correct termQuery
            return null;
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, ArrayMapper> {

        private final Mapper.Builder innerBuilder;

        public Builder(String name, Mapper.Builder innerBuilder) {
            super(name, newArrayFieldType(innerBuilder), newArrayFieldType(innerBuilder));
            this.innerBuilder = innerBuilder;
        }


        @Override
        public ArrayMapper build(BuilderContext context) {
            Mapper innerMapper = innerBuilder.build(context);
            return new ArrayMapper(name, fieldType, defaultFieldType, context.indexSettings(),
                multiFieldsBuilder.build(this, context), copyTo, innerMapper);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Object inner = node.remove(INNER_TYPE);
            if (inner == null) {
                throw new MapperParsingException("property [inner] missing");
            }
            if (!(inner instanceof Map)) {
                throw new MapperParsingException("property [inner] must be a map");
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> innerNode = (Map<String, Object>) inner;
            String typeName = (String) innerNode.get("type");
            if (typeName == null && innerNode.containsKey("properties")) {
                typeName = ObjectMapper.CONTENT_TYPE;
            } else if (CONTENT_TYPE.equalsIgnoreCase(typeName)) {
                throw new MapperParsingException("nested arrays are not supported");
            }

            Mapper.TypeParser innerTypeParser = parserContext.typeParser(typeName);
            Mapper.Builder innerBuilder = innerTypeParser.parse(name, innerNode, parserContext);

            return new Builder(name, innerBuilder);
        }
    }

    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
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
        XContentParser parser = builder.contentType().xContent().createParser(innerBuilder.bytes());

        //noinspection StatementWithEmptyBody
        while ((parser.nextToken() != XContentParser.Token.START_OBJECT)) {
            // consume tokens until start of object
        }

        //noinspection unchecked
        Map<String, Object> innerMap = (Map<String, Object>) parser.mapOrdered().get(innerMapper.simpleName());

        assert innerMap != null: "innerMap was null";

        builder.startObject(simpleName());
        builder.field("type", contentType());
        builder.field(INNER_TYPE, innerMap);
        return builder.endObject();
    }

    public Iterator<Mapper> iterator() {
        return innerMapper.iterator();
    }


    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        if (mergeWith instanceof ArrayMapper) {
            innerMapper = innerMapper.merge(((ArrayMapper) mergeWith).innerMapper, updateAllTypes);
        } else {
            innerMapper = innerMapper.merge(mergeWith, updateAllTypes);
        }
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        XContentParser parser = context.parser();
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_NULL) {
            return parseInner(context);
        } else if (token != XContentParser.Token.START_ARRAY) {
            throw new ElasticsearchParseException("invalid array");
        }
        token = parser.nextToken();
        Mapper newInnerMapper = innerMapper;
        while (token != XContentParser.Token.END_ARRAY) {
            // we only get here for non-empty arrays
            Mapper update = parseInner(context);
            if (update != null) {
                newInnerMapper = newInnerMapper.merge(update, true);
            }
            token = parser.nextToken();
        }
        if (newInnerMapper == innerMapper) {
            return null;
        }
        innerMapper = newInnerMapper;
        return this;
    }

    private Mapper parseInner(ParseContext context) throws IOException {
        Mapper update;
        if (innerMapper instanceof FieldMapper) {
            update = ((FieldMapper) innerMapper).parse(context);
        } else {
            assert innerMapper instanceof ObjectMapper : "innerMapper must be a FieldMapper or an ObjectMapper";
            context.path().add(simpleName());
            update = DocumentParser.parseObject(context, ((ObjectMapper) innerMapper), context.parser().currentName());
            context.path().remove();
        }
        return update;
    }

    private static Mapper.Builder<?, ?> detectInnerMapper(ParseContext parseContext,
                                                          String fieldName, XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_ARRAY) {
            token = parser.nextToken();
        }

        // can't use nulls to detect type
        while (token == XContentParser.Token.VALUE_NULL) {
            token = parser.nextToken();
        }

        if (token == XContentParser.Token.START_ARRAY) {
            throw new ElasticsearchParseException("nested arrays are not supported");
        } else if (token == XContentParser.Token.END_ARRAY) {
            // array is empty or has only null values
            return null;
        }

        return DocumentParser.createBuilderFromDynamicValue(parseContext, token, fieldName);
    }


    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        // parseCreateField is called in the original FieldMapper parse method.
        // Since parse is overwritten parseCreateField is never called
        throw new UnsupportedOperationException("parseCreateField not supported for " +
                                                ArrayMapper.class.getSimpleName());
    }
}
