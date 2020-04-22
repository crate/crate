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

import static io.crate.types.DataTypes.STRING;

import java.util.stream.StreamSupport;

import io.crate.analyze.user.Privilege;
import io.crate.auth.user.User;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;

public class SysPrivilegesTableInfo {

    private static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "privileges");

    @SuppressWarnings("WeakerAccess")
    public static class PrivilegeRow {
        private final String grantee;
        private final Privilege privilege;

        PrivilegeRow(String grantee, Privilege privilege) {
            this.grantee = grantee;
            this.privilege = privilege;
        }
    }

    public static SystemTable<PrivilegeRow> create() {
        return SystemTable.<PrivilegeRow>builder(IDENT)
            .add("grantee", STRING, x -> x.grantee)
            .add("grantor", STRING, x -> x.privilege.grantor())
            .add("state", STRING, x -> x.privilege.state().toString())
            .add("type", STRING, x -> x.privilege.ident().type().toString())
            .add("class", STRING, x -> x.privilege.ident().clazz().toString())
            .add("ident", STRING, x -> x.privilege.ident().ident())
            .setPrimaryKeys(
                new ColumnIdent("grantee"),
                new ColumnIdent("state"),
                new ColumnIdent("type"),
                new ColumnIdent("class"),
                new ColumnIdent("ident")
            )
            .build();
    }

    public static Iterable<PrivilegeRow> buildPrivilegesRows(Iterable<User> users) {
        return () -> StreamSupport.stream(users.spliterator(), false)
            .flatMap(u -> StreamSupport.stream(u.privileges().spliterator(), false)
                .map(p -> new PrivilegeRow(u.name(), p))
            ).iterator();
    }
}
