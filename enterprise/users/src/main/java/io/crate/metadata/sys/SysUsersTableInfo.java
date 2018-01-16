/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.metadata.sys;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.auth.user.User;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.Map;

public class SysUsersTableInfo extends StaticTableInfo {

    private static final TableIdent IDENT = new TableIdent(SysSchemaInfo.NAME, "users");
    private static final RowGranularity GRANULARITY = RowGranularity.DOC;
    private static final BytesRef PASSWORD_PLACEHOLDER = BytesRefs.toBytesRef("********");

    private static class Columns {
        private static final ColumnIdent NAME = new ColumnIdent("name");
        private static final ColumnIdent SUPERUSER = new ColumnIdent("superuser");
        private static final ColumnIdent PASSWORD = new ColumnIdent("password");
    }

    private static final ImmutableList<ColumnIdent> PRIMARY_KEY = ImmutableList.of(Columns.NAME);

    public SysUsersTableInfo() {
        super(IDENT, new ColumnRegistrar(IDENT, GRANULARITY)
                .register(Columns.NAME, DataTypes.STRING)
                .register(Columns.SUPERUSER, DataTypes.BOOLEAN)
                .register(Columns.PASSWORD, DataTypes.STRING),
            PRIMARY_KEY);
    }

    @Override
    public RowGranularity rowGranularity() {
        return GRANULARITY;
    }

    @Override
    public Routing getRouting(ClusterState state,
                              RoutingProvider routingProvider,
                              WhereClause whereClause,
                              RoutingProvider.ShardSelection shardSelection,
                              SessionContext sessionContext) {
        return Routing.forTableOnSingleNode(IDENT, state.getNodes().getLocalNodeId());
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<User>> sysUsersExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<User>>builder()
            .put(Columns.NAME, () -> new RowContextCollectorExpression<User, BytesRef>() {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.name());
                }
            })
            .put(Columns.SUPERUSER, () -> new RowContextCollectorExpression<User, Boolean>() {
                @Override
                public Boolean value() {
                    return row.isSuperUser();
                }
            })
            .put(Columns.PASSWORD, () -> new RowContextCollectorExpression<User, BytesRef>() {
                @Override
                public BytesRef value() {
                    return row.password() != null ? PASSWORD_PLACEHOLDER : null;
                }
            })
            .build();
    }
}
