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

package io.crate.metadata.view;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import org.elasticsearch.cluster.ClusterState;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ViewInfo implements TableInfo {

    private final TableIdent ident;
    private final List<Reference> columns;

    public ViewInfo(TableIdent ident, List<Reference> columns) {
        this.ident = ident;
        this.columns = columns;
    }

    @Nullable
    @Override
    public Reference getReference(ColumnIdent columnIdent) {
        return null;
    }

    @Override
    public Collection<Reference> columns() {
        return columns;
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.DOC;
    }

    @Override
    public TableIdent ident() {
        return ident;
    }

    @Override
    public Routing getRouting(ClusterState state, RoutingProvider routingProvider, WhereClause whereClause, RoutingProvider.ShardSelection shardSelection, SessionContext sessionContext) {
        return null;
    }

    @Override
    public List<ColumnIdent> primaryKey() {
        return Collections.emptyList();
    }

    @Override
    public Map<String, Object> tableParameters() {
        return Collections.emptyMap();
    }

    @Override
    public Set<Operation> supportedOperations() {
        return Operation.READ_ONLY;
    }

    @Override
    public TableType tableType() {
        return TableType.VIEW;
    }

    @Override
    public Iterator<Reference> iterator() {
        return columns.iterator();
    }

    @Override
    public String toString() {
        return ident.fqn();
    }
}
