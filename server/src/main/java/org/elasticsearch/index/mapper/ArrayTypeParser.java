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

import java.util.Map;

/**
 * TypeParser for {@code {"type": "array"}}
 */
public class ArrayTypeParser implements Mapper.TypeParser {

    @Override
    public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
        Object inner = node.remove(ArrayMapper.INNER_TYPE);
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
        }

        Mapper.TypeParser innerTypeParser = parserContext.typeParser(typeName);
        Mapper.Builder innerBuilder = innerTypeParser.parse(name, innerNode, parserContext);
        if (innerBuilder instanceof ObjectMapper.Builder) {
            return new ObjectArrayMapper.Builder(name, ((ObjectMapper.Builder) innerBuilder));
        }
        return new ArrayMapper.Builder(name, innerBuilder);
    }
}
