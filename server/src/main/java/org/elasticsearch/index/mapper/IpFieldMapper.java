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

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public IpFieldMapper build(BuilderContext context) {
            var mapper = new IpFieldMapper(
                name,
                position,
                defaultExpression,
                fieldType,
                new IpFieldType(buildFullName(context), indexed, hasDocValues),
                context.indexSettings(),
                copyTo);
            context.putPositionInfo(mapper, position);
            return mapper;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        public TypeParser() {
        }

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name);
            TypeParsers.parseField(builder, name, node, parserContext);
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
    }

    private IpFieldMapper(
            String simpleName,
            Integer position,
            String defaultExpression,
            FieldType fieldType,
            MappedFieldType mappedFieldType,
            Settings indexSettings,
            CopyTo copyTo) {
        super(simpleName, position, defaultExpression, fieldType, mappedFieldType, indexSettings, copyTo);
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
        String addressAsString = context.parser().textOrNull();
        if (addressAsString == null) {
            return;
        }
        InetAddress address = InetAddresses.forString(addressAsString);
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
    }
}
