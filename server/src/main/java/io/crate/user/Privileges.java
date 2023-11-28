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

package io.crate.user;

import org.jetbrains.annotations.Nullable;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.exceptions.MissingPrivilegeException;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.metadata.IndexParts;
import io.crate.metadata.RelationName;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;

public class Privileges {

    /**
     * Checks if the user the concrete privilege for the given class, ident and default schema, if not raise exception.
     */
    public static void ensureUserHasPrivilege(Privilege.Type type,
                                              Privilege.Clazz clazz,
                                              @Nullable String ident,
                                              User user,
                                              String defaultSchema) throws MissingPrivilegeException {
        assert user != null : "User must not be null when trying to validate privileges";
        assert type != null : "Privilege type must not be null";

        // information_schema and pg_catalog should not be protected
        if (isInformationSchema(clazz, ident) || isPgCatalogSchema(clazz, ident)) {
            return;
        }
        //noinspection PointlessBooleanExpression
        if (user.hasPrivilege(type, clazz, ident) == false) {
            boolean objectIsVisibleToUser = user.hasAnyPrivilege(clazz, ident);
            if (objectIsVisibleToUser) {
                throw new MissingPrivilegeException(user.name(), type);
            } else {
                switch (clazz) {
                    case CLUSTER:
                        throw new MissingPrivilegeException(user.name(), type);
                    case SCHEMA:
                        throw new SchemaUnknownException(ident);
                    case TABLE:
                    case VIEW:
                        RelationName relationName = RelationName.fromIndexName(ident);
                        if (user.hasAnyPrivilege(Privilege.Clazz.SCHEMA, relationName.schema())) {
                            throw new RelationUnknown(relationName);
                        } else {
                            throw new SchemaUnknownException(relationName.schema());
                        }

                    default:
                        throw new AssertionError("Invalid clazz: " + clazz);
                }
            }
        }
    }

    /**
     * Checks if the user has ANY privilege for the given class and ident, if not raise exception.
     */
    @VisibleForTesting
    public static void ensureUserHasPrivilege(Privilege.Clazz clazz,
                                              @Nullable String ident,
                                              User user) throws MissingPrivilegeException {
        assert user != null : "User must not be null when trying to validate privileges";

        // information_schema and pg_catalog should not be protected
        if (isInformationSchema(clazz, ident) || isPgCatalogSchema(clazz, ident)) {
            return;
        }
        //noinspection PointlessBooleanExpression
        if (user.hasAnyPrivilege(clazz, ident) == false) {
            switch (clazz) {
                case CLUSTER:
                    throw new MissingPrivilegeException(user.name());

                case SCHEMA:
                    throw new SchemaUnknownException(ident);

                case TABLE:
                case VIEW:
                    RelationName relationName = RelationName.fromIndexName(ident);
                    if (user.hasAnyPrivilege(Privilege.Clazz.SCHEMA, relationName.schema())) {
                        throw new RelationUnknown(relationName);
                    } else {
                        throw new SchemaUnknownException(relationName.schema());
                    }

                default:
                    throw new AssertionError("Invalid clazz: " + clazz);

            }
        }
    }

    private static String getTargetSchema(Privilege.Clazz clazz, @Nullable String ident) {
        String schemaName = null;
        if (Privilege.Clazz.CLUSTER.equals(clazz)) {
            return schemaName;
        }
        assert ident != null : "ident must not be null if privilege class is not 'CLUSTER'";
        if (Privilege.Clazz.TABLE.equals(clazz)) {
            schemaName = new IndexParts(ident).getSchema();
        } else {
            schemaName = ident;
        }
        return schemaName;
    }

    private static boolean isInformationSchema(Privilege.Clazz clazz, String ident) {
        String targetSchema = getTargetSchema(clazz, ident);
        return InformationSchemaInfo.NAME.equals(targetSchema);
    }

    private static boolean isPgCatalogSchema(Privilege.Clazz clazz, String ident) {
        String targetSchema = getTargetSchema(clazz, ident);
        return PgCatalogSchemaInfo.NAME.equals(targetSchema);
    }
}
