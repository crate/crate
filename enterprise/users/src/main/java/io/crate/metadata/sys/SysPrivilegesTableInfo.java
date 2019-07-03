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
import io.crate.analyze.user.Privilege;
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
import java.util.stream.StreamSupport;

import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static io.crate.types.DataTypes.STRING;

public class SysPrivilegesTableInfo extends StaticTableInfo<SysPrivilegesTableInfo.PrivilegeRow> {

    private static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "privileges");
    private static final RowGranularity GRANULARITY = RowGranularity.DOC;

    @SuppressWarnings("WeakerAccess")
    public static class PrivilegeRow {
        private final String grantee;
        private final Privilege privilege;

        PrivilegeRow(String grantee, Privilege privilege) {
            this.grantee = grantee;
            this.privilege = privilege;
        }
    }

    public SysPrivilegesTableInfo() {
        super(IDENT, columnRegistrar(), "grantee", "state", "type", "class","ident");
    }

    private static ColumnRegistrar<PrivilegeRow> columnRegistrar() {
        return new ColumnRegistrar<PrivilegeRow>(IDENT, GRANULARITY)
            .register("grantee", STRING, () -> forFunction(r -> r.grantee))
            .register("grantor", STRING, () -> forFunction(r -> r.privilege.grantor()))
            .register("state", STRING, () -> forFunction(r -> r.privilege.state().toString()))
            .register("type", STRING, () -> forFunction(r -> r.privilege.ident().type().toString()))
            .register("class", STRING, () -> forFunction(r -> r.privilege.ident().clazz().toString()))
            .register("ident", STRING, () -> forFunction(r -> r.privilege.ident().ident()));
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

    public static Map<ColumnIdent, RowCollectExpressionFactory<PrivilegeRow>> expressions() {
        return columnRegistrar().expressions();
    }

    public static Iterable<PrivilegeRow> buildPrivilegesRows(Iterable<User> users) {
        return () -> StreamSupport.stream(users.spliterator(), false)
            .flatMap(u -> StreamSupport.stream(u.privileges().spliterator(), false)
                .map(p -> new PrivilegeRow(u.name(), p))
            ).iterator();
    }
}
