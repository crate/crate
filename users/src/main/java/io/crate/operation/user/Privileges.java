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

import com.google.common.annotations.VisibleForTesting;
import io.crate.analyze.user.Privilege;
import io.crate.exceptions.MissingPrivilegeException;

class Privileges {

    /**
     * Checks if the user the concrete privilege for the given class and ident, if not raise exception.
     */
    static void ensureUserHasPrivilege(Privilege.Type type,
                                       Privilege.Clazz clazz,
                                       String ident,
                                       User user) throws MissingPrivilegeException {
        assert user != null : "User must not be null when trying to validate privileges";
        assert type != null : "Privilege type must not be null";

        //noinspection PointlessBooleanExpression
        if (user.hasPrivilege(type, clazz, ident) == false) {
            throw new MissingPrivilegeException(user.name(), type);
        }
    }

    /**
     * Checks if the user has ANY privilege for the given class and ident, if not raise exception.
     */
    @VisibleForTesting
    static void ensureUserHasPrivilege(Privilege.Clazz clazz,
                                       String ident,
                                       User user) throws MissingPrivilegeException {
        assert user != null : "User must not be null when trying to validate privileges";
        //noinspection PointlessBooleanExpression
        if (user.hasAnyPrivilege(clazz, ident) == false) {
            throw new MissingPrivilegeException(user.name());
        }
    }
}
