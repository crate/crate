/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.metadata;

import io.crate.exceptions.SchemaUnknownException;
import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;

public class ReferenceInfos implements Iterable<SchemaInfo>{

    public static final String DEFAULT_SCHEMA = "doc";

    private final Map<String, SchemaInfo> schemas;
    private final SchemaInfo defaultSchemaInfo;

    @Inject
    public ReferenceInfos(Map<String, SchemaInfo> schemas) {
        this.schemas = schemas;
        this.defaultSchemaInfo = schemas.get(DEFAULT_SCHEMA);
    }

    @Nullable
    public TableInfo getTableInfo(TableIdent ident) {
        SchemaInfo schemaInfo = getSchemaInfo(ident.schema());
        if (schemaInfo != null) {
            return schemaInfo.getTableInfo(ident.name());
        }
        return null;
    }

    /**
     *
     * @param ident the table ident to get a TableInfo for
     * @return an instance of TableInfo for the given ident, guaranteed to be not null
     * @throws io.crate.exceptions.SchemaUnknownException if schema given in <code>ident</code>
     *         does not exist
     * @throws io.crate.exceptions.TableUnknownException if table given in <code>ident</code> does
     *         not exist in the given schema
     */
    public TableInfo getTableInfoSafe(TableIdent ident) {
        TableInfo info;
        try {
            info = getSchemaInfoSafe(ident.schema()).getTableInfo(ident.name());
            if (info == null) {
                throw new TableUnknownException(ident.name());
            }
        } catch (Exception e) {
            throw new TableUnknownException(ident.name(), e);
        }
        return info;
    }

    /**
     * returns the schemaInfo for the given schemaName
     *
     * similar to {@link #getSchemaInfo(String)} but will throw an SchemaUnknownException
     * if the schema doesn't exist.
     */
    public SchemaInfo getSchemaInfoSafe(@Nullable String schemaName) throws SchemaUnknownException {
        if (schemaName == null) {
            return defaultSchemaInfo;
        }
        SchemaInfo schemaInfo = schemas.get(schemaName);
        if (schemaInfo == null) {
            throw new SchemaUnknownException(schemaName);
        }
        return schemaInfo;
    }

    @Nullable
    public SchemaInfo getSchemaInfo(@Nullable String schemaName) {
        if (schemaName == null) {
            return defaultSchemaInfo;
        } else {
            return schemas.get(schemaName);
        }
    }

    @Override
    public Iterator<SchemaInfo> iterator() {
        return schemas.values().iterator();
    }
}
