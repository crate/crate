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

package io.crate.analyze;

import io.crate.user.Privilege;
import io.crate.user.Privilege.State;
import io.crate.user.User;
import io.crate.common.collections.Lists2;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.SearchPath;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.sql.tree.DenyPrivilege;
import io.crate.sql.tree.GrantPrivilege;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.RevokePrivilege;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Analyzer for privileges related statements (ie GRANT/REVOKE statements)
 */
class PrivilegesAnalyzer {

    private final boolean userManagementEnabled;
    private final Schemas schemas;
    private static final String ERROR_MESSAGE = "GRANT/DENY/REVOKE Privileges on information_schema is not supported";

    PrivilegesAnalyzer(boolean userManagementEnabled, Schemas schemas) {
        this.userManagementEnabled = userManagementEnabled;
        this.schemas = schemas;
    }

    AnalyzedPrivileges analyzeGrant(GrantPrivilege node, User user, SearchPath searchPath) {
        ensureUserManagementEnabled();
        Privilege.Clazz clazz = Privilege.Clazz.valueOf(node.clazz());
        List<String> idents = validatePrivilegeIdents(clazz, node.privilegeIdents(), false, searchPath, schemas);

        return new AnalyzedPrivileges(node.userNames(),
            privilegeTypesToPrivileges(getPrivilegeTypes(node.all(), node.privileges()), user, State.GRANT, idents, clazz));
    }

    AnalyzedPrivileges analyzeRevoke(RevokePrivilege node, User user, SearchPath searchPath) {
        ensureUserManagementEnabled();
        Privilege.Clazz clazz = Privilege.Clazz.valueOf(node.clazz());
        List<String> idents = validatePrivilegeIdents(clazz, node.privilegeIdents(), true, searchPath, schemas);

        return new AnalyzedPrivileges(node.userNames(),
            privilegeTypesToPrivileges(getPrivilegeTypes(node.all(), node.privileges()), user, State.REVOKE, idents, clazz));
    }

    AnalyzedPrivileges analyzeDeny(DenyPrivilege node, User user, SearchPath searchPath) {
        ensureUserManagementEnabled();
        Privilege.Clazz clazz = Privilege.Clazz.valueOf(node.clazz());
        List<String> idents = validatePrivilegeIdents(clazz, node.privilegeIdents(), false, searchPath, schemas);

        return new AnalyzedPrivileges(node.userNames(),
            privilegeTypesToPrivileges(getPrivilegeTypes(node.all(), node.privileges()), user, State.DENY, idents, clazz));
    }

    private void ensureUserManagementEnabled() {
        if (!userManagementEnabled) {
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

    private static void validateSchemaNames(List<String> schemaNames) {
        schemaNames.forEach(PrivilegesAnalyzer::validateSchemaName);
    }

    private static void validateSchemaName(String schemaName) {
        if (InformationSchemaInfo.NAME.equals(schemaName)) {
            throw new UnsupportedFeatureException(ERROR_MESSAGE);
        }
    }

    private List<String> validatePrivilegeIdents(Privilege.Clazz clazz,
                                                 List<QualifiedName> tableOrSchemaNames,
                                                 boolean isRevoke,
                                                 SearchPath searchPath,
                                                 Schemas schemas) {
        if (Privilege.Clazz.SCHEMA.equals(clazz)) {
            List<String> schemaNames = Lists2.map(tableOrSchemaNames, QualifiedName::toString);
            if (isRevoke) {
                return schemaNames;
            }
            validateSchemaNames(schemaNames);
            return schemaNames;
        } else {
            return resolveAndValidateRelations(tableOrSchemaNames, searchPath, schemas, isRevoke);
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
                                                             List<String> idents,
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

    private static List<String> resolveAndValidateRelations(List<QualifiedName> relations,
                                                            SearchPath searchPath,
                                                            Schemas schemas,
                                                            boolean isRevoke) {
        return Lists2.map(relations, q -> {
            try {
                RelationName relationName = schemas.resolveRelation(q, searchPath);
                if (!isRevoke) {
                    validateSchemaName(relationName.schema());
                }
                return relationName.fqn();
            } catch (RelationUnknown e) {
                if (!isRevoke) {
                    throw e;
                } else {
                    return RelationName.of(q, searchPath.currentSchema()).fqn();
                }
            }
        });
    }
}
