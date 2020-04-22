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

import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.STRING;

import io.crate.auth.user.User;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;

public class SysUsersTableInfo {

    private static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "users");
    private static final String PASSWORD_PLACEHOLDER = "********";

    public static SystemTable<User> create() {
        return SystemTable.<User>builder(IDENT)
            .add("name", STRING, User::name)
            .add("superuser", BOOLEAN, User::isSuperUser)
            .add("password", STRING, x -> x.password() == null ? null : PASSWORD_PLACEHOLDER)
            .setPrimaryKeys(new ColumnIdent("name"))
            .build();
    }
}
