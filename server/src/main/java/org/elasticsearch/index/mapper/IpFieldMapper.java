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
import java.net.InetAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;


/** A {@link FieldMapper} for ip addresses. */
public class IpFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "ip";

    public static class Defaults {
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setDimensions(1, Integer.BYTES);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder> {

        private InetAddress nullValue;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
        }

        public Builder nullValue(InetAddress nullValue) {
            this.nullValue = nullValue;
            return builder;
        }

        @Override
        public IpFieldMapper build(BuilderContext context) {
            return new IpFieldMapper(
                name,
                position,
                defaultExpression,
                fieldType,
                new IpFieldType(buildFullName(context), indexed, hasDocValues),
                nullValue,
                context.indexSettings(),
                multiFieldsBuilder.build(this, context),
                copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        public TypeParser() {
        }

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name);
            TypeParsers.parseField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    builder.nullValue(InetAddresses.forString(propNode.toString()));
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class IpFieldType extends MappedFieldType {

        public IpFieldType(String name, boolean indexed, boolean hasDocValues) {
            super(name, indexed, hasDocValues);
        }

        public IpFieldType(String name) {
            this(name, true, true);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        private InetAddress parse(Object value) {
            if (value instanceof InetAddress) {
                return (InetAddress) value;
            } else {
                if (value instanceof BytesRef) {
                    value = ((BytesRef) value).utf8ToString();
                }
                return InetAddresses.forString(value.toString());
            }
        }


    }

    private final InetAddress nullValue;

    private IpFieldMapper(
            String simpleName,
            Integer position,
            String defaultExpression,
            FieldType fieldType,
            MappedFieldType mappedFieldType,
            InetAddress nullValue,
            Settings indexSettings,
            MultiFields multiFields,
            CopyTo copyTo) {
        super(simpleName, position, defaultExpression, fieldType, mappedFieldType, indexSettings, multiFields, copyTo);
        this.nullValue = nullValue;
    }

    @Override
    public IpFieldType fieldType() {
        return (IpFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return fieldType().typeName();
    }

    @Override
    protected IpFieldMapper clone() {
        return (IpFieldMapper) super.clone();
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        Object addressAsObject;
        if (context.externalValueSet()) {
            addressAsObject = context.externalValue();
        } else {
            addressAsObject = context.parser().textOrNull();
        }

        if (addressAsObject == null) {
            addressAsObject = nullValue;;
        }

        if (addressAsObject == null) {
            return;
        }

        String addressAsString = addressAsObject.toString();
        InetAddress address;
        if (addressAsObject instanceof InetAddress) {
            address = (InetAddress) addressAsObject;
        } else {
            address = InetAddresses.forString(addressAsString);
        }

        if (fieldType().isSearchable()) {
            fields.add(new InetAddressPoint(fieldType().name(), address));
        }
        if (fieldType().hasDocValues()) {
            fields.add(new SortedSetDocValuesField(fieldType().name(), new BytesRef(InetAddressPoint.encode(address))));
        } else if (fieldType.stored() || fieldType().isSearchable()) {
            createFieldNamesField(context, fields);
        }
        if (fieldType.stored()) {
            fields.add(new StoredField(fieldType().name(), new BytesRef(InetAddressPoint.encode(address))));
        }
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        IpFieldMapper mergeWith = (IpFieldMapper) other;
        if (mergeWith.nullValue != this.nullValue) {
            conflicts.add("mapper [" + name() + "] has different [null_value] values");
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (nullValue != null) {
            builder.field("null_value", InetAddresses.toAddrString(nullValue));
        }
    }
}
