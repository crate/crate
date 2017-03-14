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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * Mapper for array(object).
 *
 * Extends a ObjectMapper, but internally mostly delegates to innerMapper (which is also an ObjectMapper).
 *
 * Extends has to be used because {@link DocumentParser} does a lot of `instanceof ObjectMapper` checks. Therefore
 * it is not possible to implement only Mapper
 */
public class ObjectArrayMapper extends ObjectMapper {

    static class Builder extends ObjectMapper.Builder {

        private final ObjectMapper.Builder innerBuilder;

        Builder(String name, ObjectMapper.Builder innerBuilder) {
            super(name);
            this.innerBuilder = innerBuilder;
        }

        @Override
        public ObjectMapper build(BuilderContext context) {
            return new ObjectArrayMapper(name, innerBuilder.build(context), context.indexSettings());
        }

        @Override
        protected ObjectMapper createMapper(String name,
                                            String fullPath,
                                            boolean enabled,
                                            Nested nested,
                                            Dynamic dynamic,
                                            Boolean includeInAll,
                                            Map mappers,
                                            @Nullable Settings settings) {
            return new ObjectArrayMapper(
                name,
                super.createMapper(name, fullPath, enabled, nested, dynamic, includeInAll, mappers, settings),
                settings
            );
        }
    }

    private ObjectMapper innerMapper;

    ObjectArrayMapper(String name, ObjectMapper innerMapper, Settings settings) {
        super(name,
            innerMapper.fullPath(),
            innerMapper.isEnabled(),
            innerMapper.nested(),
            innerMapper.dynamic(),
            innerMapper.includeInAll(),
            Collections.emptyMap(),
            settings);
        this.innerMapper = innerMapper;
    }

    @Override
    public Mapper getMapper(String field) {
        return innerMapper.getMapper(field);
    }

    @Override
    protected void putMapper(Mapper mapper) {
        innerMapper.putMapper(mapper);
    }

    @Override
    public ObjectMapper updateFieldType(Map<String, MappedFieldType> fullNameToFieldType) {
        ObjectArrayMapper clone = clone();
        clone.innerMapper = innerMapper.updateFieldType(fullNameToFieldType);
        return clone;
    }

    @Override
    public ObjectMapper mappingUpdate(Mapper mapper) {
        ObjectArrayMapper clone = clone();
        clone.innerMapper = innerMapper.mappingUpdate(mapper);
        return clone;
    }

    @Override
    protected ObjectArrayMapper clone() {
        return (ObjectArrayMapper) super.clone();
    }

    @Override
    public ObjectMapper merge(Mapper mergeWith, boolean updateAllTypes) {
        ObjectArrayMapper merged = clone();

        if (mergeWith instanceof ObjectArrayMapper) {
            ObjectArrayMapper mergeWithObject = (ObjectArrayMapper) mergeWith;
            merged.innerMapper = merged.innerMapper.merge(mergeWithObject.innerMapper, updateAllTypes);
            return merged;
        }

        merged.innerMapper = merged.innerMapper.merge(mergeWith, updateAllTypes);
        return merged;
    }

    @Override
    protected void doMerge(ObjectMapper mergeWith, boolean updateAllTypes) {
        // unused because merge is overwritten
        throw new UnsupportedOperationException("doMerge not supported");
    }

    @Override
    public void toXContent(XContentBuilder builder, Params params, ToXContent custom) throws IOException {
        ArrayMapper.toXContent(builder, params, innerMapper, simpleName(), ArrayMapper.CONTENT_TYPE);
    }

    @Override
    public Iterator<Mapper> iterator() {
        return innerMapper.iterator();
    }
}
