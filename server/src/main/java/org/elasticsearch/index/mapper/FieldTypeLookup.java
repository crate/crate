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

import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * An immutable container for looking up {@link MappedFieldType}s by their name.
 */
class FieldTypeLookup implements Iterable<MappedFieldType> {

    /**
     * A mapping from concrete field name to field type.
     **/
    private final Map<String, MappedFieldType> fullNameToFieldType = new HashMap<>();

    FieldTypeLookup() {
        this(Collections.emptyList());
    }

    FieldTypeLookup(Collection<FieldMapper> fieldMappers) {
        for (FieldMapper fieldMapper : fieldMappers) {
            MappedFieldType fieldType = fieldMapper.fieldType();
            var name = fieldMapper.columnOID == COLUMN_OID_UNASSIGNED ? fieldType.name() : Long.toString(fieldMapper.columnOID);
            fullNameToFieldType.put(name, fieldType);
        }
    }

    /** Returns the field for the given field */
    public MappedFieldType get(String field) {
        return fullNameToFieldType.get(field);
    }

    @Override
    public Iterator<MappedFieldType> iterator() {
        return fullNameToFieldType.values().iterator();
    }
}
