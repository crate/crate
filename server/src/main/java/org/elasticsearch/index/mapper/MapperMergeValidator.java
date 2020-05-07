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
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;


/**
 * A utility class that helps validate certain aspects of a mappings update.
 */
class MapperMergeValidator {

    /**
     * Validates the overall structure of the mapping addition, including whether
     * duplicate fields are present, and if the provided fields have already been
     * defined with a different data type.
     *
     * @param type The mapping type, for use in error messages.
     * @param objectMappers The newly added object mappers.
     * @param fieldMappers The newly added field mappers.
     * @param fieldAliasMappers The newly added field alias mappers.
     * @param fullPathObjectMappers All object mappers, indexed by their full path.
     * @param fieldTypes All field and field alias mappers, collected into a lookup structure.
     */
    public static void validateMapperStructure(String type,
                                               Collection<ObjectMapper> objectMappers,
                                               Collection<FieldMapper> fieldMappers,
                                               Collection<FieldAliasMapper> fieldAliasMappers,
                                               Map<String, ObjectMapper> fullPathObjectMappers,
                                               FieldTypeLookup fieldTypes,
                                               boolean updateAllTypes) {
        checkFieldUniqueness(type, objectMappers, fieldMappers,
            fieldAliasMappers, fullPathObjectMappers, fieldTypes);
        checkObjectsCompatibility(objectMappers, fullPathObjectMappers, updateAllTypes);
    }

    private static void checkFieldUniqueness(String type,
                                             Collection<ObjectMapper> objectMappers,
                                             Collection<FieldMapper> fieldMappers,
                                             Collection<FieldAliasMapper> fieldAliasMappers,
                                             Map<String, ObjectMapper> fullPathObjectMappers,
                                             FieldTypeLookup fieldTypes) {

        // first check within mapping
        Set<String> objectFullNames = new HashSet<>();
        for (ObjectMapper objectMapper : objectMappers) {
            String fullPath = objectMapper.fullPath();
            if (objectFullNames.add(fullPath) == false) {
                throw new IllegalArgumentException("Object mapper [" + fullPath + "] is defined twice in mapping for type [" + type + "]");
            }
        }

        Set<String> fieldNames = new HashSet<>();
        Stream.concat(fieldMappers.stream(), fieldAliasMappers.stream())
            .forEach(mapper -> {
                String name = mapper.name();
                if (objectFullNames.contains(name)) {
                    throw new IllegalArgumentException("Field [" + name + "] is defined both as an object and a field in [" + type + "]");
                } else if (fieldNames.add(name) == false) {
                    throw new IllegalArgumentException("Field [" + name + "] is defined twice in [" + type + "]");
                }
            });

        // then check other types
        for (String fieldName : fieldNames) {
            if (fullPathObjectMappers.containsKey(fieldName)) {
                throw new IllegalArgumentException("[" + fieldName + "] is defined as a field in mapping [" + type
                    + "] but this name is already used for an object in other types");
            }
        }

        for (String objectPath : objectFullNames) {
            if (fieldTypes.get(objectPath) != null) {
                throw new IllegalArgumentException("[" + objectPath + "] is defined as an object in mapping [" + type
                    + "] but this name is already used for a field in other types");
            }
        }
    }

    private static void checkObjectsCompatibility(Collection<ObjectMapper> objectMappers,
                                                  Map<String, ObjectMapper> fullPathObjectMappers,
                                                  boolean updateAllTypes) {
        for (ObjectMapper newObjectMapper : objectMappers) {
            ObjectMapper existingObjectMapper = fullPathObjectMappers.get(newObjectMapper.fullPath());
            if (existingObjectMapper != null) {
                // simulate a merge and ignore the result, we are just interested
                // in exceptions here
                existingObjectMapper.merge(newObjectMapper, updateAllTypes);
            }
        }
    }
}
