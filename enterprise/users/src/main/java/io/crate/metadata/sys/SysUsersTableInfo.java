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

import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.auth.user.User;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import org.elasticsearch.cluster.ClusterState;

import java.util.Map;

import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.BOOLEAN;

public class SysUsersTableInfo extends StaticTableInfo<User> {

    private static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "users");
    private static final RowGranularity GRANULARITY = RowGranularity.DOC;
    private static final String PASSWORD_PLACEHOLDER = "********";

    public SysUsersTableInfo() {
        super(IDENT, columnRegistrar(), "name");
    }

    private static ColumnRegistrar<User> columnRegistrar() {
        return new ColumnRegistrar<User>(IDENT, GRANULARITY)
            .register("name", STRING, () -> forFunction(User::name))
            .register("superuser", BOOLEAN, () -> forFunction(User::isSuperUser))
            .register("password", STRING, () -> forFunction(u -> u.password() != null ? PASSWORD_PLACEHOLDER : null));
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

    public static Map<ColumnIdent, RowCollectExpressionFactory<User>> expressions() {
        return columnRegistrar().expressions();
    }
}
