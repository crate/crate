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

package io.crate.es.index.mapper.array;

import io.crate.es.index.mapper.Mapper;
import io.crate.es.index.mapper.ObjectMapper;
import io.crate.es.index.mapper.ParseContext;

/**
 * marker interface for a factory that creates Mapper.Builder
 * which can handle a DynamicArrayField.
 *
 * This is used to create a Mapper.Builder which builds a mapper that is
 * capable of handling dynamic arrays.
 * Bind an implementation of this interface in a plugin to provide a
 * mapper that is then used to parse dynamic arrays and create the appropriate mappings
 * and field- and/or object-mappers.
 */
public interface DynamicArrayFieldMapperBuilderFactory {

    Mapper create(String name, ObjectMapper parentMapper, ParseContext context);
}
