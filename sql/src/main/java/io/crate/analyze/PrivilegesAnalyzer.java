/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze;

import io.crate.analyze.user.Privilege;
import io.crate.analyze.user.Privilege.State;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.operation.user.User;
import io.crate.sql.tree.DenyPrivilege;
import io.crate.sql.tree.GrantPrivilege;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.RevokePrivilege;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Analyzer for privileges related statements (ie GRANT/REVOKE statements)
 */
class PrivilegesAnalyzer {

    private final Schemas schemas;

    PrivilegesAnalyzer(Schemas schemas) {
        this.schemas = schemas;
    }

    PrivilegesAnalyzedStatement analyzeGrant(GrantPrivilege node, @Nullable User user) {
        ensureUserManagementEnabled(user);
        validatePrivilegeIdents(Privilege.Clazz.valueOf(node.clazz()), node.privilegeIdents());

        return new PrivilegesAnalyzedStatement(node.userNames(),
            privilegeTypesToPrivileges(getPrivilegeTypes(node.all(), node.privileges()), user, State.GRANT, node.privilegeIdents(),
                Privilege.Clazz.valueOf(node.clazz())));
    }

    PrivilegesAnalyzedStatement analyzeRevoke(RevokePrivilege node, @Nullable User user) {
        ensureUserManagementEnabled(user);
        validatePrivilegeIdents(Privilege.Clazz.valueOf(node.clazz()), node.privilegeIdents());

        return new PrivilegesAnalyzedStatement(node.userNames(),
            privilegeTypesToPrivileges(getPrivilegeTypes(node.all(), node.privileges()), user, State.REVOKE, node.privilegeIdents(),
                Privilege.Clazz.valueOf(node.clazz())));
    }

    PrivilegesAnalyzedStatement analyzeDeny(DenyPrivilege node, @Nullable User user) {
        ensureUserManagementEnabled(user);
        validatePrivilegeIdents(Privilege.Clazz.valueOf(node.clazz()), node.privilegeIdents());

        return new PrivilegesAnalyzedStatement(node.userNames(),
            privilegeTypesToPrivileges(getPrivilegeTypes(node.all(), node.privileges()), user, State.DENY, node.privilegeIdents(),
                Privilege.Clazz.valueOf(node.clazz())));
    }

    private static void ensureUserManagementEnabled(@Nullable User user) {
        if (user == null) {
            throw new UnsupportedOperationException("User management is not enabled");
        }
    }

    private static Collection<Privilege.Type> getPrivilegeTypes(boolean all, List<String> typeNames) {
        Collection<Privilege.Type> privilegeTypes;
        if (all) {
            privilegeTypes = Privilege.Type.VALUES;
        } else {
            privilegeTypes = parsePrivilegeTypes(typeNames);
        }
        return privilegeTypes;
    }

    private void validateTableNames(List<QualifiedName> tableNames) {
        tableNames.forEach(t -> schemas.getTableInfo(TableIdent.fromIndexName(t.toString()), null));
    }

    private void validateSchemaNames(List<QualifiedName> schemaNames) {
        schemaNames.forEach(s ->
            schemas.validateSchemaName(s.toString())
        );
    }

    private void validatePrivilegeIdents(Privilege.Clazz clazz, List<QualifiedName> idents) {
        if (Privilege.Clazz.SCHEMA.equals(clazz)) {
            validateSchemaNames(idents);
        } else if (Privilege.Clazz.TABLE.equals(clazz)) {
            validateTableNames(idents);
        }
    }

    private static List<Privilege.Type> parsePrivilegeTypes(List<String> privilegeTypeNames) {
        List<Privilege.Type> privilegeTypes = new ArrayList<>(privilegeTypeNames.size());
        for (String typeName : privilegeTypeNames) {
            Privilege.Type privilegeType;
            try {
                privilegeType = Privilege.Type.valueOf(typeName.toUpperCase(Locale.ENGLISH));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Unknown privilege type '%s'", typeName));
            }
            //noinspection PointlessBooleanExpression
            if (Privilege.Type.VALUES.contains(privilegeType) == false) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Unknown privilege type '%s'", typeName));
            }
            privilegeTypes.add(privilegeType);
        }
        return privilegeTypes;
    }

    private static Set<Privilege> privilegeTypesToPrivileges(Collection<Privilege.Type> privilegeTypes,
                                                             User grantor,
                                                             State state,
                                                             List<QualifiedName> tableOrSchemaNames,
                                                             Privilege.Clazz clazz) {
        Set<Privilege> privileges = new HashSet<>(privilegeTypes.size());
        if (Privilege.Clazz.CLUSTER.equals(clazz)) {
            for (Privilege.Type privilegeType : privilegeTypes) {
                Privilege privilege = new Privilege(state,
                    privilegeType,
                    clazz,
                    null,
                    grantor.name()
                );
                privileges.add(privilege);
            }
        } else {
            List<String> idents = convertQualifiedNamesToIdents(clazz, tableOrSchemaNames);
            for (Privilege.Type privilegeType : privilegeTypes) {
                for (String ident : idents) {
                    Privilege privilege = new Privilege(state,
                        privilegeType,
                        clazz,
                        ident,
                        grantor.name()
                    );

                    privileges.add(privilege);
                }
            }
        }
        return privileges;
    }

    private static List<String> convertQualifiedNamesToIdents(Privilege.Clazz clazz,
                                                              List<QualifiedName> tableOrSchemaNames) {
        if (clazz.equals(Privilege.Clazz.SCHEMA)) {
            return tableOrSchemaNames.stream().map(QualifiedName::toString).collect(Collectors.toList());
        }
        return tableOrSchemaNames.stream().map(q ->
            TableIdent.of(q, Schemas.DEFAULT_SCHEMA_NAME).fqn()).collect(Collectors.toList());
    }
}
