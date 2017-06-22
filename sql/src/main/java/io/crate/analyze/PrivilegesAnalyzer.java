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
import io.crate.operation.user.User;
import io.crate.sql.tree.GrantPrivilege;
import io.crate.sql.tree.RevokePrivilege;
import io.crate.sql.tree.DenyPrivilege;

import javax.annotation.Nullable;
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

    static PrivilegesAnalyzedStatement analyzeGrant(GrantPrivilege node, @Nullable User user) {
        ensureUserManagementEnabled(user);
        return new PrivilegesAnalyzedStatement(node.userNames(),
            privilegeTypesToPrivileges(getPrivilegeTypes(node.all(), node.privileges()), user, State.GRANT));
    }

    static PrivilegesAnalyzedStatement analyzeRevoke(RevokePrivilege node, @Nullable User user) {
        ensureUserManagementEnabled(user);
        return new PrivilegesAnalyzedStatement(node.userNames(),
            privilegeTypesToPrivileges(getPrivilegeTypes(node.all(), node.privileges()), user, State.REVOKE));
    }

    static PrivilegesAnalyzedStatement analyzeDeny(DenyPrivilege node, @Nullable User user) {
        ensureUserManagementEnabled(user);
        return new PrivilegesAnalyzedStatement(node.userNames(),
            privilegeTypesToPrivileges(getPrivilegeTypes(node.all(), node.privileges()), user, State.DENY));
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
                                                      State state) {
        Set<Privilege> privileges = new HashSet<>(privilegeTypes.size());
        for (Privilege.Type privilegeType : privilegeTypes) {
            Privilege privilege = new Privilege(state,
                privilegeType,
                Privilege.Clazz.CLUSTER,
                null,
                grantor.name()
            );
            privileges.add(privilege);
        }
        return privileges;
    }
}
