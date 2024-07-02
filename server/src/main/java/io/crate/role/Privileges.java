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

package io.crate.role;

import org.jetbrains.annotations.Nullable;

import org.jetbrains.annotations.VisibleForTesting;
import io.crate.exceptions.MissingPrivilegeException;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.metadata.IndexParts;
import io.crate.metadata.RelationName;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;

public final class Privileges {

    private Privileges() {}

    /**
     * Checks if the user the concrete privilege for the given securable, ident and default schema, if not raise exception.
     */
    public static void ensureUserHasPrivilege(Roles roles,
                                              Role user,
                                              Permission permission,
                                              Securable securable,
                                              @Nullable String ident) throws MissingPrivilegeException {
        assert roles != null : "Roles must not be null when trying to validate privileges";
        assert user != null : "User must not be null when trying to validate privileges";
        assert permission != null : "Permission must not be null";

        // information_schema and pg_catalog should not be protected
        if (isInformationSchema(securable, ident) || isPgCatalogSchema(securable, ident)) {
            return;
        }
        //noinspection PointlessBooleanExpression
        if (roles.hasPrivilege(user, permission, securable, ident) == false) {
            boolean objectIsVisibleToUser = roles.hasAnyPrivilege(user, securable, ident);
            if (objectIsVisibleToUser) {
                throw new MissingPrivilegeException(user.name(), permission);
            } else {
                switch (securable) {
                    case CLUSTER:
                        throw new MissingPrivilegeException(user.name(), permission);
                    case SCHEMA:
                        throw new SchemaUnknownException(ident);
                    case TABLE:
                    case VIEW:
                        RelationName relationName = RelationName.fromIndexName(ident);
                        if (roles.hasAnyPrivilege(user, Securable.SCHEMA, relationName.schema())) {
                            throw new RelationUnknown(relationName);
                        } else {
                            throw new SchemaUnknownException(relationName.schema());
                        }

                    default:
                        throw new AssertionError("Invalid securable: " + securable);
                }
            }
        }
    }

    /**
     * Checks if the user has ANY privilege for the given securable and ident, if not raise exception.
     */
    @VisibleForTesting
    public static void ensureUserHasPrivilege(Roles roles,
                                              Role user,
                                              Securable securable,
                                              @Nullable String ident) throws MissingPrivilegeException {
        assert roles != null : "Roles must not be null when trying to validate privileges";
        assert user != null : "User must not be null when trying to validate privileges";

        // information_schema and pg_catalog should not be protected
        if (isInformationSchema(securable, ident) || isPgCatalogSchema(securable, ident)) {
            return;
        }
        //noinspection PointlessBooleanExpression
        if (roles.hasAnyPrivilege(user, securable, ident) == false) {
            switch (securable) {
                case CLUSTER:
                    throw new MissingPrivilegeException(user.name());

                case SCHEMA:
                    throw new SchemaUnknownException(ident);

                case TABLE:
                case VIEW:
                    RelationName relationName = RelationName.fromIndexName(ident);
                    if (roles.hasAnyPrivilege(user, Securable.SCHEMA, relationName.schema())) {
                        throw new RelationUnknown(relationName);
                    } else {
                        throw new SchemaUnknownException(relationName.schema());
                    }

                default:
                    throw new AssertionError("Invalid securable: " + securable);

            }
        }
    }

    private static String getTargetSchema(Securable securable, @Nullable String ident) {
        String schemaName = null;
        if (Securable.CLUSTER.equals(securable)) {
            return schemaName;
        }
        assert ident != null : "ident must not be null if privilege securable is not 'CLUSTER'";
        if (Securable.TABLE.equals(securable)) {
            schemaName = new IndexParts(ident).getSchema();
        } else {
            schemaName = ident;
        }
        return schemaName;
    }

    private static boolean isInformationSchema(Securable securable, String ident) {
        String targetSchema = getTargetSchema(securable, ident);
        return InformationSchemaInfo.NAME.equals(targetSchema);
    }

    private static boolean isPgCatalogSchema(Securable securable, String ident) {
        String targetSchema = getTargetSchema(securable, ident);
        return PgCatalogSchemaInfo.NAME.equals(targetSchema);
    }
}
