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

package io.crate.metadata.information;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.Routing;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.AbstractTableInfo;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.planner.RowGranularity;

import javax.annotation.Nullable;
import java.util.*;

public class InformationTableInfo extends AbstractTableInfo {

    protected final TableIdent ident;
    private final ImmutableList<ColumnIdent> primaryKeyIdentList;
    protected final Routing routing;

    private final ImmutableMap<ColumnIdent, ReferenceInfo> references;
    private final ImmutableList<ReferenceInfo> columns;
    private final String[] concreteIndices;


    protected InformationTableInfo(InformationSchemaInfo schemaInfo,
                                   TableIdent ident,
                                   ImmutableList<ColumnIdent> primaryKeyIdentList,
                                   LinkedHashMap<ColumnIdent, ReferenceInfo> references) {
        this(schemaInfo, ident, primaryKeyIdentList, references, null);
    }

    protected InformationTableInfo(InformationSchemaInfo schemaInfo,
                                   TableIdent ident,
                                   ImmutableList<ColumnIdent> primaryKeyIdentList,
                                   LinkedHashMap<ColumnIdent, ReferenceInfo> references,
                                   @Nullable ImmutableList<ReferenceInfo> columns) {
        super(schemaInfo);
        this.ident = ident;
        this.primaryKeyIdentList = primaryKeyIdentList;
        this.references = ImmutableMap.copyOf(references);
        this.columns = columns != null ? columns : ImmutableList.copyOf(references.values());
        this.concreteIndices = new String[]{ident.esName()};
        Map<String, Map<String, List<Integer>>> locations = new TreeMap<>();
        Map<String, List<Integer>> tableLocation = new TreeMap<>();
        tableLocation.put(ident.fqn(), null);
        locations.put(NULL_NODE_ID, tableLocation);
        this.routing = new Routing(locations);
    }

    @Nullable
    @Override
    public ReferenceInfo getReferenceInfo(ColumnIdent columnIdent) {
        return references.get(columnIdent);
    }

    @Override
    public Collection<ReferenceInfo> columns() {
        return columns;
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.DOC;
    }

    @Override
    public Routing getRouting(WhereClause whereClause, @Nullable String preference) {
        return routing;
    }

    @Override
    public TableIdent ident() {
        return ident;
    }

    @Override
    public List<ColumnIdent> primaryKey() {
        return primaryKeyIdentList;
    }

    @Override
    public String[] concreteIndices() {
        return concreteIndices;
    }

    @Override
    public ColumnPolicy columnPolicy() {
        return ColumnPolicy.STRICT;
    }

    @Override
    public Iterator<ReferenceInfo> iterator() {
        return references.values().iterator();
    }
}
