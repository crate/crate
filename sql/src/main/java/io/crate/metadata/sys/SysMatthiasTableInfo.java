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

package io.crate.metadata.sys;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.WhereClause;
import io.crate.metadata.*;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterService;

import javax.annotation.Nullable;
import java.util.*;

public class SysMatthiasTableInfo extends SysTableInfo {

    public static final String TABLE_NAME = "matthias";
    public static final TableIdent IDENT = new TableIdent(SCHEMA, TABLE_NAME);

    public static final String NAME = "name";
    public static final String FROM = "from";
    public static final String TITLE = "title";
    public static final String TO = "to";
    public static final String STATS = "stats";
    public static final String COMMITS = "commits"; // 888
    public static final String LINES_ADDED = "lines_added";
    public static final String LINES_REMOVED = "lines_removed";
    public static final String HEARTS_BROKEN = "hearts_broken"; // +infinity
    public static final String FAVORITE_PROGRAMMING_LANGUAGE = "favorite_programming_language";
    public static final String MESSAGE = "message";
    public static final String GIFS = "gifs";

    private static final ImmutableList<ColumnIdent> PRIMARY_KEY = ImmutableList.of(new ColumnIdent(NAME));

    private final Map<ColumnIdent, ReferenceInfo> infos;
    private final Set<ReferenceInfo> columns;

    protected SysMatthiasTableInfo(ClusterService clusterService) {
        super(clusterService);

        ColumnRegistrar registrar = new ColumnRegistrar(IDENT, RowGranularity.CLUSTER)
                .register(NAME, DataTypes.STRING, null)
                .register(FROM, DataTypes.TIMESTAMP, null)
                .register(TO, DataTypes.TIMESTAMP, null)
                .register(TITLE, DataTypes.STRING, null)
                .register(STATS, DataTypes.OBJECT, null)
                .register(STATS, DataTypes.LONG, ImmutableList.of(COMMITS))
                .register(STATS, DataTypes.LONG, ImmutableList.of(LINES_ADDED))
                .register(STATS, DataTypes.LONG, ImmutableList.of(LINES_REMOVED))
                .register(STATS, DataTypes.DOUBLE, ImmutableList.of(HEARTS_BROKEN))
                .register(FAVORITE_PROGRAMMING_LANGUAGE, DataTypes.STRING, null)
                .register(MESSAGE, DataTypes.STRING, null)
                .register(GIFS, new ArrayType(DataTypes.STRING), null);

        infos = registrar.infos();
        columns = registrar.columns();

    }

    @Nullable
    @Override
    public ReferenceInfo getReferenceInfo(ColumnIdent columnIdent) {
        return infos.get(columnIdent);
    }

    @Override
    public Collection<ReferenceInfo> columns() {
        return columns;
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.CLUSTER;
    }

    @Override
    public TableIdent ident() {
        return IDENT;
    }

    @Override
    public Routing getRouting(WhereClause whereClause, @Nullable String preference) {
        return Routing.forTableOnNode(IDENT, clusterService.localNode().id());
    }

    @Override
    public List<ColumnIdent> primaryKey() {
        return PRIMARY_KEY;
    }

    @Override
    public Iterator<ReferenceInfo> iterator() {
        return infos.values().iterator();
    }
}
