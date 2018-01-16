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

package io.crate.auth.user;

import io.crate.analyze.user.Privilege;
import io.crate.exceptions.ClusterScopeException;
import io.crate.exceptions.CrateException;
import io.crate.exceptions.CrateExceptionVisitor;
import io.crate.exceptions.MissingPrivilegeException;
import io.crate.exceptions.SchemaScopeException;
import io.crate.exceptions.TableScopeException;
import io.crate.exceptions.UnscopedException;
import io.crate.metadata.TableIdent;

import java.util.Locale;

class ExceptionPrivilegeValidator implements ExceptionAuthorizedValidator {

    private static final Visitor VISITOR = new Visitor();

    private final User user;

    ExceptionPrivilegeValidator(User user) {
        this.user = user;
    }

    @Override
    public void ensureExceptionAuthorized(Throwable t) throws MissingPrivilegeException {
        if (t instanceof CrateException) {
            VISITOR.process((CrateException) t, user);
        }
    }

    private static class Visitor extends CrateExceptionVisitor<User, Void> {

        @Override
        protected Void visitCrateException(CrateException e, User context) {
            throw new IllegalStateException(String.format(Locale.ENGLISH,
                "CrateException '%s' not supported by privileges exception validator", e.getClass()));
        }

        @Override
        protected Void visitTableScopeException(TableScopeException e, User context) {
            for (TableIdent tableIdent : e.getTableIdents()) {
                Privileges.ensureUserHasPrivilege(Privilege.Clazz.TABLE, tableIdent.toString(), context);
            }
            return null;
        }

        @Override
        protected Void visitSchemaScopeException(SchemaScopeException e, User context) {
            Privileges.ensureUserHasPrivilege(Privilege.Clazz.SCHEMA, e.getSchemaName(), context);
            return null;
        }

        @Override
        protected Void visitClusterScopeException(ClusterScopeException e, User context) {
            Privileges.ensureUserHasPrivilege(Privilege.Clazz.CLUSTER, null, context);
            return null;
        }

        @Override
        protected Void visitUnscopedException(UnscopedException e, User context) {
            return null;
        }
    }
}
