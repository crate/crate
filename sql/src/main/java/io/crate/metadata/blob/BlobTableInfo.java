/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.collect.ImmutableList;
import io.crate.Version;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.AlterBlobTableParameterInfo;
import io.crate.analyze.TableParameterInfo;
import io.crate.analyze.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.ShardedTable;
import io.crate.metadata.table.StoredTable;
import io.crate.metadata.table.TableInfo;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.collect.Tuple;

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

    private final TableIdent ident;
    private final int numberOfShards;
    private final BytesRef numberOfReplicas;
    private final String index;
    private final LinkedHashSet<Reference> columns = new LinkedHashSet<>();
    private final BytesRef blobsPath;
    private final TableParameterInfo tableParameterInfo;
    private final Map<String, Object> tableParameters;
    private final String routingHashFunction;
    private final Version versionCreated;
    private final Version versionUpgraded;
    private final boolean closed;

    private static final Map<ColumnIdent, Reference> INFOS = new LinkedHashMap<>();

    private static final ImmutableList<ColumnIdent> PRIMARY_KEY = ImmutableList.of(new ColumnIdent("digest"));
    private static final List<Tuple<String, DataType>> STATIC_COLUMNS = ImmutableList.<Tuple<String, DataType>>builder()
        .add(new Tuple<>("digest", DataTypes.STRING))
        .add(new Tuple<>("last_modified", DataTypes.TIMESTAMP))
        .build();

    public BlobTableInfo(TableIdent ident,
                         String index,
                         int numberOfShards,
                         BytesRef numberOfReplicas,
                         Map<String, Object> tableParameters,
                         BytesRef blobsPath,
                         String routingHashFunction,
                         @Nullable Version versionCreated,
                         @Nullable Version versionUpgraded,
                         boolean closed) {
        this.ident = ident;
        this.index = index;
        this.numberOfShards = numberOfShards;
        this.numberOfReplicas = numberOfReplicas;
        this.blobsPath = blobsPath;
        this.tableParameterInfo = new AlterBlobTableParameterInfo();
        this.tableParameters = tableParameters;
        this.routingHashFunction = routingHashFunction;
        this.versionCreated = versionCreated;
        this.versionUpgraded = versionUpgraded;
        this.closed = closed;

        registerStaticColumns();
    }

    @Nullable
    @Override
    public Reference getReference(ColumnIdent columnIdent) {
        return INFOS.get(columnIdent);
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
    public BytesRef numberOfReplicas() {
        return numberOfReplicas;
    }

    @Nullable
    @Override
    public ColumnIdent clusteredBy() {
        return PRIMARY_KEY.get(0);
    }

    @Override
    public Iterator<Reference> iterator() {
        return columns.iterator();
    }

    private void registerStaticColumns() {
        for (Tuple<String, DataType> column : STATIC_COLUMNS) {
            Reference ref = new Reference(
                new ReferenceIdent(ident(), column.v1(), null), RowGranularity.DOC, column.v2());
            assert ref.ident().isColumn() : "only top-level columns should be added to columns list";
            columns.add(ref);
            INFOS.put(ref.ident().columnIdent(), ref);
        }
    }

    public BytesRef blobsPath() {
        return blobsPath;
    }

    public TableParameterInfo tableParameterInfo() {
        return tableParameterInfo;
    }

    public Map<String, Object> tableParameters() {
        return tableParameters;
    }

    @Override
    public Set<Operation> supportedOperations() {
        return Operation.BLOB_OPERATIONS;
    }

    @Override
    public String routingHashFunction() {
        return routingHashFunction;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public String[] concreteIndices() {
        return new String[] {index};
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
