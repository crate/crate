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

package io.crate.metadata.table;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.TableIdent;

import javax.annotation.Nullable;
import java.util.*;

public abstract class StaticTableInfo implements TableInfo {

    private final TableIdent ident;
    private final List<ColumnIdent> primaryKey;
    private final Collection<Reference> columns;
    protected final Map<ColumnIdent, Reference> columnMap;

    /**
     * @param columns top level columns. If null the values of columnMap are used.
     *                Can/should be specified if columnMap contains nested columns.
     */
    public StaticTableInfo(TableIdent ident,
                           Map<ColumnIdent, Reference> columnMap,
                           @Nullable Collection<Reference> columns,
                           List<ColumnIdent> primaryKey) {
        this.ident = ident;
        this.columnMap = columnMap;
        this.columns = columns == null ? columnMap.values() : columns;
        this.primaryKey = primaryKey;
    }

    public StaticTableInfo(TableIdent ident, ColumnRegistrar columnRegistrar, List<ColumnIdent> primaryKey) {
        this(ident, columnRegistrar.infos(), columnRegistrar.columns(), primaryKey);
    }

    @Nullable
    @Override
    public Reference getReference(ColumnIdent columnIdent) {
        return columnMap.get(columnIdent);
    }

    @Override
    public Collection<Reference> columns() {
        return columns;
    }

    @Override
    public TableIdent ident() {
        return ident;
    }

    @Override
    public List<ColumnIdent> primaryKey() {
        return primaryKey;
    }

    @Override
    public Map<String, Object> tableParameters() {
        return Collections.emptyMap();
    }

    @Override
    public String toString() {
        return ident.fqn();
    }

    @Override
    public Iterator<Reference> iterator() {
        return columnMap.values().iterator();
    }

    @Override
    public Set<Operation> supportedOperations() {
        return Operation.SYS_READ_ONLY;
    }

}
