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
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterState;

import java.util.Map;
import java.util.stream.StreamSupport;

import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;

public class SysPrivilegesTableInfo extends StaticTableInfo {

    private static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "privileges");
    private static final RowGranularity GRANULARITY = RowGranularity.DOC;

    private static class Columns {
        static final ColumnIdent GRANTEE = new ColumnIdent("grantee");
        static final ColumnIdent GRANTOR = new ColumnIdent("grantor");
        static final ColumnIdent STATE = new ColumnIdent("state");
        static final ColumnIdent TYPE = new ColumnIdent("type");
        static final ColumnIdent CLASS = new ColumnIdent("class");
        static final ColumnIdent IDENT = new ColumnIdent("ident");
    }

    private static final ImmutableList<ColumnIdent> PRIMARY_KEY = ImmutableList.of(
        Columns.GRANTEE, Columns.STATE, Columns.TYPE, Columns.CLASS, Columns.IDENT);

    public static class PrivilegeRow {
        private final String grantee;
        private final Privilege privilege;

        PrivilegeRow(String grantee, Privilege privilege) {
            this.grantee = grantee;
            this.privilege = privilege;
        }
    }


    public SysPrivilegesTableInfo() {
        super(IDENT, new ColumnRegistrar(IDENT, GRANULARITY)
                .register(Columns.GRANTEE, DataTypes.STRING)
                .register(Columns.GRANTOR, DataTypes.STRING)
                .register(Columns.STATE, DataTypes.STRING)
                .register(Columns.TYPE, DataTypes.STRING)
                .register(Columns.CLASS, DataTypes.STRING)
                .register(Columns.IDENT, DataTypes.STRING),
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

    public static Map<ColumnIdent, RowCollectExpressionFactory<PrivilegeRow>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<PrivilegeRow>>builder()
            .put(Columns.GRANTEE, () -> forFunction(r -> r.grantee))
            .put(Columns.GRANTOR, () -> forFunction(r -> r.privilege.grantor()))
            .put(Columns.STATE, () -> forFunction(r -> r.privilege.state().toString()))
            .put(Columns.TYPE, () -> forFunction(r -> r.privilege.ident().type().toString()))
            .put(Columns.CLASS, () -> forFunction(r -> r.privilege.ident().clazz().toString()))
            .put(Columns.IDENT, () -> forFunction(r -> r.privilege.ident().ident()))
            .build();
    }

    public static Iterable<PrivilegeRow> buildPrivilegesRows(Iterable<User> users) {
        return () -> StreamSupport.stream(users.spliterator(), false)
            .flatMap(u -> StreamSupport.stream(u.privileges().spliterator(), false)
                .map(p -> new PrivilegeRow(u.name(), p))
            ).iterator();
    }
}
