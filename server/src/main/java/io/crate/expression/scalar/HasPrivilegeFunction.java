/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.expression.scalar;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.elasticsearch.common.TriFunction;
import org.jetbrains.annotations.Nullable;

import io.crate.data.Input;
import io.crate.exceptions.MissingPrivilegeException;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.role.Privilege;
import io.crate.role.Role;
import io.crate.role.RoleLookup;

public abstract class HasPrivilegeFunction extends Scalar<Boolean, Object> {

    private final BiFunction<RoleLookup, Object, Role> getUser;

    private final TriFunction<Role, Object, Collection<Privilege.Type>, Boolean> checkPrivilege;

    protected static final BiFunction<RoleLookup, Object, Role> USER_BY_NAME = (userLookup, userName) -> {
        var user = userLookup.findUser((String) userName);
        if (user == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "User %s does not exist", userName));
        }
        return user;
    };

    protected static final BiFunction<RoleLookup, Object, Role> USER_BY_OID = (userLookup, userOid) -> {
        var user = userLookup.findUser((Integer) userOid);
        if (user == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "User with OID %d does not exist", userOid));
        }
        return user;
    };

    /**
     * @param privilege is a comma separated list.
     * Valid privileges are 'CREATE' and 'USAGE' which map to DDL and DQL correspondingly.
     * Case of the privilege string is not significant, and extra whitespace is allowed between privilege names.
     * Repetition of the valid argument is allowed.
     *
     * @throws IllegalArgumentException if privilege contains invalid privilege.
     * @return collection of privileges parsed
     */
    @Nullable
    protected abstract Collection<Privilege.Type> parsePrivileges(String privilege);

    protected HasPrivilegeFunction(Signature signature,
                                   BoundSignature boundSignature,
                                   BiFunction<RoleLookup, Object, Role> getUser,
                                   TriFunction<Role, Object, Collection<Privilege.Type>, Boolean> checkPrivilege) {
        super(signature, boundSignature);
        this.getUser = getUser;
        this.checkPrivilege = checkPrivilege;
    }

    @Override
    public Symbol normalizeSymbol(io.crate.expression.symbol.Function symbol, TransactionContext txnCtx, NodeContext nodeCtx) {
        return evaluateIfLiterals(this, txnCtx, nodeCtx, symbol);
    }

    @Override
    public Scalar<Boolean, Object> compile(List<Symbol> arguments, String currentUser, RoleLookup userLookup) {
        // When possible, user is looked up only once.
        // Privilege string normalization/mapping into CrateDB Privilege.Type is also done once if possible
        Object userValue = null;
        Symbol privileges = null;
        if (arguments.size() == 2) {
            userValue = currentUser;
            privileges = arguments.get(1);
        }
        if (arguments.size() == 3) {
            if (arguments.get(0) instanceof Input<?> input) {
                userValue = input.value();
            }
            privileges = arguments.get(2);
        }

        Collection<Privilege.Type> compiledPrivileges = normalizePrivilegeIfLiteral(privileges);
        if (userValue == null) {
            // Fall to non-compiled version which returns null.
            return this;
        }

        // Compiled privileges can be null here but we don't fall to non-compiled version as
        // can mean that privilege string is not null but not Literal either.
        // When we pass NULL to the compiled version, it treats last argument like regular evaluate:
        // does null check and parses privileges string.
        var sessionUser = USER_BY_NAME.apply(userLookup, currentUser);
        Role user = getUser.apply(userLookup, userValue);
        validateCallPrivileges(sessionUser, user);
        return new CompiledHasPrivilege(signature, boundSignature, sessionUser, user, compiledPrivileges);
    }


    /**
     * @return List of Privilege.Type compiled from inout or NULL if cannot be compiled.
     */
    @Nullable
    private Collection<Privilege.Type> normalizePrivilegeIfLiteral(Symbol symbol) {
        if (symbol instanceof Input<?> input) {
            var value = input.value();
            if (value == null) {
                return null;
            }
            return parsePrivileges((String) value);
        }
        return null;
    }

    @Override
    public final Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>[] args) {
        Object userNameOrOid, schemaNameOrOid, privileges;

        var sessionUser = USER_BY_NAME.apply(nodeCtx.userLookup(), txnCtx.sessionSettings().userName());
        Role user;
        if (args.length == 2) {
            schemaNameOrOid = args[0].value();
            privileges = args[1].value();
            user = sessionUser;
        } else {
            userNameOrOid = args[0].value();
            if (userNameOrOid == null) {
                return null;
            }
            user = getUser.apply(nodeCtx.userLookup(), userNameOrOid);
            validateCallPrivileges(sessionUser, user);
            schemaNameOrOid = args[1].value();
            privileges = args[2].value();
        }

        if (schemaNameOrOid == null || privileges == null) {
            return null;
        }
        return checkPrivilege.apply(user, schemaNameOrOid, parsePrivileges((String) privileges));
    }

    private class CompiledHasPrivilege extends Scalar<Boolean, Object> {

        private final Role sessionUser;
        private final Role user;

        // We don't use String to avoid unnecessary cast of the ignored argument
        // when function provides pre-computed results
        private final Function<Object, Collection<Privilege.Type>> getPrivileges;

        private CompiledHasPrivilege(Signature signature,
                                     BoundSignature boundSignature,
                                     Role sessionUser,
                                     Role user,
                                     @Nullable Collection<Privilege.Type> compiledPrivileges) {
            super(signature, boundSignature);
            this.sessionUser = sessionUser;
            this.user = user;
            if (compiledPrivileges != null) {
                getPrivileges = s -> compiledPrivileges;
            } else {
                getPrivileges = s -> parsePrivileges((String) s);
            }
        }

        @Override
        @SafeVarargs
        public final Boolean evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<Object>... args) {
            Object schema, privilege;
            if (args.length == 2) {
                // User is taken from the session
                schema = args[0].value();
                privilege = args[1].value();
            } else {
                // args[0] is resolved to a user
                validateCallPrivileges(sessionUser, user);
                schema = args[1].value();
                privilege = args[2].value();
            }
            if (schema == null || privilege == null) {
                return null;
            }
            return checkPrivilege.apply(user, schema, getPrivileges.apply(privilege));
        }
    }

    protected static void validateCallPrivileges(Role sessionUser, Role user) {
        // Only superusers can call this function for other users
        if (user.name().equals(sessionUser.name()) == false
            && sessionUser.hasPrivilege(Privilege.Type.DQL, Privilege.Clazz.TABLE, "sys.privileges") == false
            && sessionUser.hasPrivilege(Privilege.Type.AL, Privilege.Clazz.CLUSTER, "crate") == false) {
            throw new MissingPrivilegeException(sessionUser.name());
        }
    }
}
