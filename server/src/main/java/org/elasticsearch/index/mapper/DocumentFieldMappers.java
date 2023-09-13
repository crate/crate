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

public final class DocumentFieldMappers implements Iterable<Mapper> {

    /** Full field name to mapper */
    private final Map<String, Mapper> fieldMappers;

    public DocumentFieldMappers(Collection<FieldMapper> mappers) {
        Map<String, Mapper> fieldMappers = new HashMap<>();
        for (FieldMapper mapper : mappers) {
            var name = mapper.columnOID() == COLUMN_OID_UNASSIGNED ? mapper.name() : Long.toString(mapper.columnOID());
            fieldMappers.put(name, mapper);
        }
        this.fieldMappers = Collections.unmodifiableMap(fieldMappers);
    }

    /**
     * Returns the leaf mapper associated with this field name. Note that the returned mapper
     * could be either a concrete {@link FieldMapper}, or a {@link FieldAliasMapper}.
     *
     * To access a field's type information, {@link MapperService#fullName} should be used instead.
     */
    public Mapper getMapper(String field) {
        return fieldMappers.get(field);
    }

    public Iterator<Mapper> iterator() {
        return fieldMappers.values().iterator();
    }
}
