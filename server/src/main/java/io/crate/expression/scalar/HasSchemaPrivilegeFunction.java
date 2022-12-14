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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.elasticsearch.common.TriFunction;

import io.crate.data.Input;
import io.crate.exceptions.MissingPrivilegeException;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogTableDefinitions;
import io.crate.types.DataTypes;
import io.crate.user.Privilege;
import io.crate.user.User;
import io.crate.user.UserLookup;

public class HasSchemaPrivilegeFunction extends Scalar<Boolean, Object> {

    public static final String NAME = "has_schema_privilege";

    private final Signature signature;
    private final BoundSignature boundSignature;

    private BiFunction<UserLookup, Object, User> getUser;

    private TriFunction<User, Object, Collection<Privilege.Type>, Boolean> checkPrivilege;

    private static final BiFunction<UserLookup, Object, User> USER_BY_NAME = (userLookup, userName) -> {
        var user = userLookup.findUser((String) userName);
        if (user == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "User %s does not exist", userName));
        }
        return user;
    };

    private static final BiFunction<UserLookup, Object, User> USER_BY_OID = (userLookup, userOid) -> {
        var user = userLookup.findUser((Integer) userOid);
        if (user == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "User with OID %d does not exist", userOid));
        }
        return user;
    };

    private static final TriFunction<User, Object, Collection<Privilege.Type>, Boolean> CHECK_BY_SCHEMA_NAME = (user, schema, privileges) -> {
        String schemaName = (String) schema;
        boolean result = false;
        for (Privilege.Type type: privileges) {
            // USAGE is allowed for public schemas
            if (type == Privilege.Type.DQL && PgCatalogTableDefinitions.isPgCatalogOrInformationSchema(schemaName)) {
                return true;
            }
            // Last argument is null as it's not used for Privilege.Clazz.SCHEMA
            result |= user.hasPrivilege(type, Privilege.Clazz.SCHEMA, schemaName, null);
        }
        return result;
    };

    private static final TriFunction<User, Object, Collection<Privilege.Type>, Boolean> CHECK_BY_SCHEMA_OID = (user, schema, privileges) -> {
        Integer schemaOid = (Integer) schema;
        boolean result = false;
        for (Privilege.Type type: privileges) {
            // USAGE is allowed for public schemas
            if (type == Privilege.Type.DQL && PgCatalogTableDefinitions.isPgCatalogOrInformationSchema(schemaOid)) {
                return true;
            }
            result |= user.hasSchemaPrivilege(type, schemaOid); // Returns true if has any privilege out of 2 possible
        }
        return result;
    };

    /**
     * @param privilege is a comma separated list.
     * Valid privileges are 'CREATE' and 'USAGE' which map to DDL and DQL correspondingly.
     * Case of the privilege string is not significant, and extra whitespace is allowed between privilege names.
     * Repetition of the valid argument is allowed.
     *
     * @throws IllegalArgumentException if privilege contains invalid privilege.
     * @return
     */
    @Nullable
    private static Collection<Privilege.Type> parsePrivileges(String privilege) {
        Collection<Privilege.Type> toCheck = new HashSet<>();
        String[] privileges = privilege.toLowerCase(Locale.ENGLISH).split(",");
        for (int i = 0; i < privileges.length; i++) {
            privileges[i] = privileges[i].trim();
            if (privileges[i].equals("create")) {
                toCheck.add(Privilege.Type.DDL);
            } else if (privileges[i].equals("usage")) {
                toCheck.add(Privilege.Type.DQL);
            } else {
                // Same error as PG
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Unrecognized privilege type: %s", privileges[i]));
            }
        }
        return toCheck;
    }

    public static void register(ScalarFunctionModule module) {
        // Signature without user, takes user from session.
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(), // Schema
                DataTypes.STRING.getTypeSignature(), // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasSchemaPrivilegeFunction(signature, boundSignature, USER_BY_NAME, CHECK_BY_SCHEMA_NAME)
        );

        // Signature without user, takes user from session.
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.INTEGER.getTypeSignature(), // Schema
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasSchemaPrivilegeFunction(signature, boundSignature, USER_BY_NAME, CHECK_BY_SCHEMA_OID)
        );

        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(), // User
                DataTypes.STRING.getTypeSignature(), // Schema
                DataTypes.STRING.getTypeSignature(), // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasSchemaPrivilegeFunction(signature, boundSignature, USER_BY_NAME, CHECK_BY_SCHEMA_NAME)
        );

        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),  // User
                DataTypes.INTEGER.getTypeSignature(), // Schema
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasSchemaPrivilegeFunction(signature, boundSignature, USER_BY_NAME, CHECK_BY_SCHEMA_OID)
        );

        module.register(
            Signature.scalar(
                NAME,
                DataTypes.INTEGER.getTypeSignature(), // User
                DataTypes.STRING.getTypeSignature(),  // Schema
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasSchemaPrivilegeFunction(signature, boundSignature, USER_BY_OID, CHECK_BY_SCHEMA_NAME)
        );

        module.register(
            Signature.scalar(
                NAME,
                DataTypes.INTEGER.getTypeSignature(), // User
                DataTypes.INTEGER.getTypeSignature(), // Schema
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasSchemaPrivilegeFunction(signature, boundSignature, USER_BY_OID, CHECK_BY_SCHEMA_OID)
        );

    }



    private HasSchemaPrivilegeFunction(Signature signature,
                                       BoundSignature boundSignature,
                                       BiFunction<UserLookup, Object, User> getUser,
                                       TriFunction<User, Object, Collection<Privilege.Type>, Boolean> checkPrivilege) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.getUser = getUser;
        this.checkPrivilege = checkPrivilege;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public BoundSignature boundSignature() {
        return boundSignature;
    }

    @Override
    public Symbol normalizeSymbol(io.crate.expression.symbol.Function symbol, TransactionContext txnCtx, NodeContext nodeCtx) {
        return evaluateIfLiterals(this, txnCtx, nodeCtx, symbol);
    }

    @Override
    public Scalar<Boolean, Object> compile(List<Symbol> arguments, String currentUser, UserLookup userLookup) {
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
        User user = getUser.apply(userLookup, userValue);
        validateCallPrivileges(sessionUser, user);
        return new CompiledHasSchemaPrivilege(user, compiledPrivileges);
    }

    @Nullable
    /**
     * @return List of Privilege.Type compiled from inout or NULL if cannot be compiled.
     */
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
    public final Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
        Object userNameOrOid, schemaNameOrOid, privileges;

        var sessionUser = USER_BY_NAME.apply(nodeCtx.userLookup(), txnCtx.sessionSettings().userName());
        User user;
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

    private class CompiledHasSchemaPrivilege extends Scalar<Boolean, Object> {

        private final User user;

        // We don't use String to avoid unnecessary cast of the ignored argument
        // when function provides pre-computed results
        private final Function<Object, Collection<Privilege.Type>> getPrivileges;

        private CompiledHasSchemaPrivilege(User user, @Nullable Collection<Privilege.Type> compiledPrivileges) {
            this.user = user;
            if (compiledPrivileges != null) {
                getPrivileges = s -> compiledPrivileges;
            } else {
                getPrivileges = s -> parsePrivileges((String) s);
            }
        }

        @Override
        public Signature signature() {
            return signature;
        }

        @Override
        public BoundSignature boundSignature() {
            return boundSignature;
        }

        @Override
        public Boolean evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<Object>... args) {
            Object schema, privilege;
            if (args.length == 2) {
                // User is taken from the session
                schema = args[0].value();
                privilege = args[1].value();
            } else {
                // args[0] is resolved to a user
                schema = args[1].value();
                privilege = args[2].value();
            }
            if (schema == null || privilege == null) {
                return null;
            }
            return checkPrivilege.apply(user, schema, getPrivileges.apply(privilege));
        }
    }

    private static void validateCallPrivileges(User sessionUser, User user) {
        // Only superusers can call this function for other users
        if (user.name().equals(sessionUser.name()) == false
            && sessionUser.hasPrivilege(Privilege.Type.DQL, Privilege.Clazz.TABLE, "sys.privileges", null) == false
            && sessionUser.hasPrivilege(Privilege.Type.AL, Privilege.Clazz.CLUSTER, "crate", null) == false) {
            throw new MissingPrivilegeException(sessionUser.name());
        }
    }
}
