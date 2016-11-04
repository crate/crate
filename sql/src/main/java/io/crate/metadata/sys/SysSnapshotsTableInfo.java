/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.metadata.sys;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Locale;
import java.util.Random;

public class SysSnapshotsTableInfo extends StaticTableInfo {

    public static final TableIdent IDENT = new TableIdent(SysSchemaInfo.NAME, "snapshots");
    private final ClusterService clusterService;

    public static class Columns {
        public static final ColumnIdent NAME = new ColumnIdent("name");
        public static final ColumnIdent REPOSITORY = new ColumnIdent("repository");
        public static final ColumnIdent CONCRETE_INDICES = new ColumnIdent("concrete_indices");
        public static final ColumnIdent STARTED = new ColumnIdent("started");
        public static final ColumnIdent FINISHED = new ColumnIdent("finished");
        public static final ColumnIdent VERSION = new ColumnIdent("version");
        public static final ColumnIdent STATE = new ColumnIdent("state");
    }

    private static final ImmutableList<ColumnIdent> PRIMARY_KEY = ImmutableList.of(Columns.NAME, Columns.REPOSITORY);
    private static final RowGranularity GRANULARITY = RowGranularity.DOC;

    private Random random = new Random();

    public SysSnapshotsTableInfo(ClusterService clusterService) {
        super(IDENT, new ColumnRegistrar(IDENT, GRANULARITY)
                .register(Columns.NAME, DataTypes.STRING)
                .register(Columns.REPOSITORY, DataTypes.STRING)
                .register(Columns.CONCRETE_INDICES, new ArrayType(DataTypes.STRING))
                .register(Columns.STARTED, DataTypes.TIMESTAMP)
                .register(Columns.FINISHED, DataTypes.TIMESTAMP)
                .register(Columns.VERSION, DataTypes.STRING)
                .register(Columns.STATE, DataTypes.STRING),
            PRIMARY_KEY);
        this.clusterService = clusterService;
    }

    @Override
    public RowGranularity rowGranularity() {
        return GRANULARITY;
    }

    @Override
    public TableIdent ident() {
        return IDENT;
    }

    @Override
    public Routing getRouting(WhereClause whereClause, @Nullable String preference) {
        // route to random master or data node,
        // because RepositoriesService (and so snapshots info) is only available there
        return Routing.forTableOnSingleNode(IDENT, randomMasterOrDataNode().getId());
    }

    private DiscoveryNode randomMasterOrDataNode() {
        ImmutableOpenMap<String, DiscoveryNode> masterAndDataNodes = clusterService.state().nodes().getMasterAndDataNodes();
        int randomIdx = random.nextInt(masterAndDataNodes.size());
        Iterator<DiscoveryNode> it = masterAndDataNodes.valuesIt();
        int currIdx = 0;
        while (it.hasNext()) {
            if (currIdx == randomIdx) {
                return it.next();
            }
            currIdx++;
        }
        throw new AssertionError(String.format(Locale.ENGLISH,
            "Cannot find a master or data node with given random index %d", randomIdx));
    }
}
