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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.jetbrains.annotations.NotNull;

import io.crate.common.collections.Lists;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.RelationInfo;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.SearchPath;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.table.Operation;
import io.crate.role.GrantedRolesChange;
import io.crate.role.Permission;
import io.crate.role.Policy;
import io.crate.role.Privilege;
import io.crate.role.Role;
import io.crate.role.Securable;
import io.crate.sql.tree.DenyPrivilege;
import io.crate.sql.tree.GrantPrivilege;
import io.crate.sql.tree.PrivilegeStatement;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.RevokePrivilege;

/**
 * Analyzer for privileges related statements (ie GRANT/REVOKE statements)
 */
class PrivilegesAnalyzer {

    private final Schemas schemas;
    private static final String ERROR_MESSAGE = "GRANT/DENY/REVOKE Privileges on information_schema is not supported";

    PrivilegesAnalyzer(Schemas schemas) {
        this.schemas = schemas;
    }

    AnalyzedPrivileges analyzeGrant(GrantPrivilege node, Role grantor, SearchPath searchPath) {
        return buildAnalyzedPrivileges(node, grantor, searchPath);
    }

    AnalyzedPrivileges analyzeRevoke(RevokePrivilege node, Role grantor, SearchPath searchPath) {
        return buildAnalyzedPrivileges(node, grantor, searchPath);
    }

    AnalyzedPrivileges analyzeDeny(DenyPrivilege node, Role grantor, SearchPath searchPath) {
        return buildAnalyzedPrivileges(node, grantor, searchPath);
    }

    @NotNull
    private AnalyzedPrivileges buildAnalyzedPrivileges(PrivilegeStatement node, Role grantor, SearchPath searchPath) {
        Policy policy;
        switch (node) {
            case GrantPrivilege ignored -> policy = Policy.GRANT;
            case RevokePrivilege ignored -> policy = Policy.REVOKE;
            case DenyPrivilege ignored -> policy = Policy.DENY;
        }
        Securable securable = Securable.valueOf(node.securable());
        List<String> idents = validatePrivilegeIdents(
            grantor,
            securable,
            node.privilegeIdents(),
            policy == Policy.REVOKE,
            searchPath,
            schemas);


        if (securable == Securable.CLUSTER && node.all() == false) {
            List<Permission> permissions = parsePermissions(node.privileges(), false);
            if (permissions.isEmpty() == false) {
                if (permissions.size() != node.privileges().size()) {
                    throw new IllegalArgumentException("Mixing up cluster privileges with roles is not allowed");
                } else {
                    return AnalyzedPrivileges.ofPrivileges(node.userNames(),
                        permissionsToPrivileges(getPermissions(node.all(), node.privileges()),
                            grantor,
                            policy,
                            idents,
                            securable));
                }
            }

            if (policy == Policy.DENY) {
                throw new IllegalArgumentException("Cannot DENY a role");
            }
            if (node.userNames().contains(Role.CRATE_USER.name())) {
                throw new IllegalArgumentException("Cannot grant roles to " + Role.CRATE_USER.name() + " superuser");
            }
            if (node.privileges().contains(Role.CRATE_USER.name())) {
                throw new IllegalArgumentException("Cannot grant " + Role.CRATE_USER.name() + " superuser, to other " +
                    "users or roles");
            }

            for (var grantee : node.userNames()) {
                for (var roleNameToGrant : node.privileges()) {
                    if (roleNameToGrant.equals(grantee)) {
                        throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                            "Cannot grant role %s to itself as a cycle will be created", grantee));
                    }
                }
            }
            return AnalyzedPrivileges.ofRolePrivileges(
                node.userNames(),
                new GrantedRolesChange(policy, new HashSet<>(node.privileges()),
                    grantor.name()));
        } else {
            return AnalyzedPrivileges.ofPrivileges(node.userNames(),
                permissionsToPrivileges(
                    getPermissions(node.all(), node.privileges()),
                    grantor,
                    policy,
                    idents,
                    securable));
        }
    }

    private static Collection<Permission> getPermissions(boolean all, List<String> permissionNames) {
        Collection<Permission> permissions;
        if (all) {
            permissions = Permission.VALUES;
        } else {
            permissions = parsePermissions(permissionNames, true);
        }
        return permissions;
    }

    private static void validateSchemaNames(List<String> schemaNames) {
        schemaNames.forEach(PrivilegesAnalyzer::validateSchemaName);
    }

    private static void validateSchemaName(String schemaName) {
        if (InformationSchemaInfo.NAME.equals(schemaName)) {
            throw new UnsupportedFeatureException(ERROR_MESSAGE);
        }
    }

    private List<String> validatePrivilegeIdents(Role sessionUser,
                                                 Securable securable,
                                                 List<QualifiedName> tableOrSchemaNames,
                                                 boolean isRevoke,
                                                 SearchPath searchPath,
                                                 Schemas schemas) {
        if (Securable.SCHEMA.equals(securable)) {
            List<String> schemaNames = Lists.map(tableOrSchemaNames, QualifiedName::toString);
            if (isRevoke) {
                return schemaNames;
            }
            validateSchemaNames(schemaNames);
            return schemaNames;
        } else {
            return resolveAndValidateRelations(tableOrSchemaNames, sessionUser, searchPath, schemas, isRevoke);
        }
    }

    private static List<Permission> parsePermissions(List<String> permissionNames, boolean validate) {
        List<Permission> permissions = new ArrayList<>(permissionNames.size());
        for (String permissionName : permissionNames) {
            Permission permission;
            try {
                permission = Permission.valueOf(permissionName.toUpperCase(Locale.ENGLISH));
                permissions.add(permission);
            } catch (IllegalArgumentException e) {
                if (validate) {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "Unknown permission '%s'", permissionName));
                }
            }
        }
        return permissions;
    }

    private static Set<Privilege> permissionsToPrivileges(Collection<Permission> permissions,
                                                          Role grantor,
                                                          Policy policy,
                                                          List<String> idents,
                                                          Securable securable) {
        Set<Privilege> privileges = new HashSet<>(permissions.size());
        if (Securable.CLUSTER.equals(securable)) {
            for (Permission permission : permissions) {
                Privilege privilege = new Privilege(
                    policy,
                    permission,
                    securable,
                    null,
                    grantor.name()
                );
                privileges.add(privilege);
            }
        } else {
            for (Permission permission : permissions) {
                for (String ident : idents) {
                    Privilege privilege = new Privilege(
                        policy,
                        permission,
                        securable,
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
                                                            Role sessionUser,
                                                            SearchPath searchPath,
                                                            Schemas schemas,
                                                            boolean isRevoke) {
        return Lists.map(relations, q -> {
            try {
                RelationInfo relation = schemas.findRelation(q, Operation.READ, sessionUser, searchPath);
                RelationName relationName = relation.ident();
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
