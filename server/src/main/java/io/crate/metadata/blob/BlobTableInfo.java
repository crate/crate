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

package io.crate.metadata.blob;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.TableParameters;
import io.crate.analyze.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.ShardedTable;
import io.crate.metadata.table.StoredTable;
import io.crate.metadata.table.TableInfo;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import io.crate.common.collections.Tuple;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BlobTableInfo implements TableInfo, ShardedTable, StoredTable {

    private final RelationName ident;
    private final int numberOfShards;
    private final String numberOfReplicas;
    private final String index;
    private final LinkedHashSet<SimpleReference> columns = new LinkedHashSet<>();
    private final String blobsPath;
    private final TableParameters supportedTableParameters;
    private final Settings tableParameters;
    private final Version versionCreated;
    private final Version versionUpgraded;
    private final boolean closed;

    private final Map<ColumnIdent, SimpleReference> infos = new LinkedHashMap<>();

    private static final List<ColumnIdent> PRIMARY_KEY = List.of(new ColumnIdent("digest"));
    private static final List<Tuple<String, DataType>> STATIC_COLUMNS = List.of(
        new Tuple<>("digest", DataTypes.STRING),
        new Tuple<>("last_modified", DataTypes.TIMESTAMPZ)
    );

    public BlobTableInfo(RelationName ident,
                         String index,
                         int numberOfShards,
                         String numberOfReplicas,
                         Settings tableParameters,
                         String blobsPath,
                         @Nullable Version versionCreated,
                         @Nullable Version versionUpgraded,
                         boolean closed) {
        assert ident.indexNameOrAlias().equals(index) : "RelationName indexName must match index";
        this.ident = ident;
        this.index = index;
        this.numberOfShards = numberOfShards;
        this.numberOfReplicas = numberOfReplicas;
        this.blobsPath = blobsPath;
        this.supportedTableParameters = TableParameters.ALTER_BLOB_TABLE_PARAMETERS;
        this.tableParameters = tableParameters;
        this.versionCreated = versionCreated;
        this.versionUpgraded = versionUpgraded;
        this.closed = closed;

        registerStaticColumns();
    }

    @Nullable
    @Override
    public SimpleReference getReference(ColumnIdent columnIdent) {
        return infos.get(columnIdent);
    }

    @Override
    public Collection<SimpleReference> columns() {
        return columns;
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.DOC;
    }

    @Override
    public RelationName ident() {
        return ident;
    }

    @Override
    public Routing getRouting(ClusterState state,
                              RoutingProvider routingProvider,
                              WhereClause whereClause,
                              RoutingProvider.ShardSelection shardSelection,
                              SessionContext sessionContext) {
        return routingProvider.forIndices(state, new String[] { index }, Collections.emptyMap(), false, shardSelection);
    }

    @Override
    public List<ColumnIdent> primaryKey() {
        return PRIMARY_KEY;
    }

    @Override
    public int numberOfShards() {
        return numberOfShards;
    }

    @Override
    public String numberOfReplicas() {
        return numberOfReplicas;
    }

    @Nullable
    @Override
    public ColumnIdent clusteredBy() {
        return PRIMARY_KEY.get(0);
    }

    @Override
    public Iterator<SimpleReference> iterator() {
        return columns.iterator();
    }

    private void registerStaticColumns() {
        int pos = 0;
        for (Tuple<String, DataType> column : STATIC_COLUMNS) {
            SimpleReference ref = new SimpleReference(
                new ReferenceIdent(ident(), column.v1(), null), RowGranularity.DOC, column.v2(), pos, null
            );
            assert ref.column().isTopLevel() : "only top-level columns should be added to columns list";
            pos++;
            columns.add(ref);
            infos.put(ref.column(), ref);
        }
    }

    public String blobsPath() {
        return blobsPath;
    }

    public TableParameters tableParameters() {
        return supportedTableParameters;
    }

    public Settings parameters() {
        return tableParameters;
    }

    @Override
    public Set<Operation> supportedOperations() {
        return Operation.BLOB_OPERATIONS;
    }

    @Override
    public RelationType relationType() {
        return RelationType.BASE_TABLE;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public String[] concreteIndices() {
        return new String[] { index };
    }

    @Nullable
    @Override
    public Version versionCreated() {
        return versionCreated;
    }

    @Nullable
    @Override
    public Version versionUpgraded() {
        return versionUpgraded;
    }

}
