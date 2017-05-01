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

package io.crate.operation.user;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import org.apache.lucene.util.BytesRef;

import java.util.Map;
import java.util.Objects;

public class User {

    private final String name;
    private final boolean superuser;

    public User(String name, boolean superuser) {
        this.name = name;
        this.superuser = superuser;
    }

    public String name() {
        return name;
    }

    public boolean superuser() {
        return superuser;
    }

    static Map<ColumnIdent, RowCollectExpressionFactory> sysUsersExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
            .put(SysUsersTableInfo.Columns.NAME, () -> new RowContextCollectorExpression<User, BytesRef>() {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.name());
                }
            })
            .put(SysUsersTableInfo.Columns.SUPERUSER, () -> new RowContextCollectorExpression<User, Boolean>() {
                @Override
                public Boolean value() {
                    return row.superuser();
                }
            })
            .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        User that = (User)o;
        return Objects.equals(name, that.name) &&
               Objects.equals(superuser, that.superuser);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, superuser);
    }
}
