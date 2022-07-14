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

import java.util.Iterator;
import java.util.Map;

import org.elasticsearch.cluster.metadata.ColumnPositionResolver;
import org.elasticsearch.common.settings.Settings;

public class RootObjectMapper extends ObjectMapper {

    private ColumnPositionResolver<Mapper> columnPositionResolver = new ColumnPositionResolver<>();

    public static class Builder extends ObjectMapper.Builder<Builder> {

        public Builder(String name) {
            super(name);
            this.builder = this;
        }

        @Override
        public RootObjectMapper build(BuilderContext context) {
            return (RootObjectMapper) super.build(context);
        }

        @Override
        protected ObjectMapper createMapper(String name, int position, String fullPath, Dynamic dynamic,
                Map<String, Mapper> mappers, Settings settings) {
            return new RootObjectMapper(
                name,
                dynamic,
                mappers,
                settings
            );
        }
    }

    public static class TypeParser extends ObjectMapper.TypeParser {

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            RootObjectMapper.Builder builder = new Builder(name);
            Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (parseObjectOrDocumentTypeProperties(fieldName, fieldNode, parserContext, builder)) {
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    RootObjectMapper(String name,
                     Dynamic dynamic,
                     Map<String, Mapper> mappers,
                     Settings settings) {
        super(name, NOT_TO_BE_POSITIONED, name, dynamic, mappers, settings);
    }

    @Override
    public RootObjectMapper merge(Mapper mergeWith) {
        RootObjectMapper newMapper = (RootObjectMapper) super.merge(mergeWith);
        if (mergeWith instanceof RootObjectMapper rootObjectMapper) {
            rootObjectMapper.columnPositionResolver.updatePositions(this.maxColumnPosition());
        }
        return newMapper;
    }

    public void updateColumnPositionResolver(ColumnPositionResolver<Mapper> columnPositionResolver) {
        this.columnPositionResolver = columnPositionResolver;
    }
}
