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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;


/**
 * A utility class that helps validate certain aspects of a mappings update.
 */
class MapperMergeValidator {

    /**
     * Validates the new mapping addition, checking whether duplicate entries are present and if the
     * provided fields are compatible with the mappings that are already defined.
     *
     * @param objectMappers The newly added object mappers.
     * @param fieldMappers The newly added field mappers.
     * @param fieldAliasMappers The newly added field alias mappers.
     * @param fieldTypes Any existing field and field alias mappers, collected into a lookup structure.
     */
    public static void validateNewMappers(Collection<ObjectMapper> objectMappers,
                                          Collection<FieldMapper> fieldMappers,
                                          Collection<FieldAliasMapper> fieldAliasMappers,
                                          FieldTypeLookup fieldTypes) {
        Set<String> objectFullNames = new HashSet<>();
        for (ObjectMapper objectMapper : objectMappers) {
            String fullPath = objectMapper.fullPath();
            if (objectFullNames.add(fullPath) == false) {
                throw new IllegalArgumentException("Object mapper [" + fullPath + "] is defined twice.");
            }
        }

        Set<String> fieldNames = new HashSet<>();
        for (FieldMapper fieldMapper : fieldMappers) {
            String name = fieldMapper.name();
            if (objectFullNames.contains(name)) {
                throw new IllegalArgumentException("Field [" + name + "] is defined both as an object and a field.");
            } else if (fieldNames.add(name) == false) {
                throw new IllegalArgumentException("Field [" + name + "] is defined twice.");
            }
        }

        Set<String> fieldAliasNames = new HashSet<>();
        for (FieldAliasMapper fieldAliasMapper : fieldAliasMappers) {
            String name = fieldAliasMapper.name();
            if (objectFullNames.contains(name)) {
                throw new IllegalArgumentException("Field [" + name + "] is defined both as an object and a field.");
            } else if (fieldNames.contains(name)) {
                throw new IllegalArgumentException("Field [" + name + "] is defined both as an alias and a concrete field.");
            } else if (fieldAliasNames.add(name) == false) {
                throw new IllegalArgumentException("Field [" + name + "] is defined twice.");
            }

            validateFieldAliasMapper(name, fieldAliasMapper.path(), fieldNames, fieldAliasNames);
        }
    }

    /**
     * Checks that the new field alias is valid.
     *
     * Note that this method assumes that new concrete fields have already been processed, so that it
     * can verify that an alias refers to an existing concrete field.
     */
    private static void validateFieldAliasMapper(String aliasName,
                                                 String path,
                                                 Set<String> fieldMappers,
                                                 Set<String> fieldAliasMappers) {
        if (path.equals(aliasName)) {
            throw new IllegalArgumentException("Invalid [path] value [" + path + "] for field alias [" +
                aliasName + "]: an alias cannot refer to itself.");
        }

        if (fieldAliasMappers.contains(path)) {
            throw new IllegalArgumentException("Invalid [path] value [" + path + "] for field alias [" +
                aliasName + "]: an alias cannot refer to another alias.");
        }

        if (fieldMappers.contains(path) == false) {
            throw new IllegalArgumentException("Invalid [path] value [" + path + "] for field alias [" +
                aliasName + "]: an alias must refer to an existing field in the mappings.");
        }
    }
}
