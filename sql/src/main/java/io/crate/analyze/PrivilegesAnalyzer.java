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
import io.crate.operation.user.UserManager;
import io.crate.sql.tree.DenyPrivilege;
import io.crate.sql.tree.GrantPrivilege;
import io.crate.sql.tree.RevokePrivilege;
import org.elasticsearch.ResourceNotFoundException;

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

    private final UserManager userManager;

    PrivilegesAnalyzer(UserManager userManager) {
        this.userManager = userManager;
    }

    PrivilegesAnalyzedStatement analyzeGrant(GrantPrivilege node, User user) {
        validateUsernames(node.userNames());
        Collection<Privilege.Type> privilegeTypes;
        if (node.all()) {
            privilegeTypes = Privilege.GRANTABLE_TYPES;
        } else {
            privilegeTypes = parsePrivilegeTypes(node.privileges());
        }
        return new PrivilegesAnalyzedStatement(node.userNames(),
            privilegeTypesToPrivileges(privilegeTypes, user, State.GRANT));
    }

    PrivilegesAnalyzedStatement analyzeDeny(DenyPrivilege node, User user) {
        validateUsernames(node.userNames());
        Collection<Privilege.Type> privilegeTypes;
        if (node.all()) {
            privilegeTypes = Privilege.GRANTABLE_TYPES;
        } else {
            privilegeTypes = parsePrivilegeTypes(node.privileges());
        }
        return new PrivilegesAnalyzedStatement(node.userNames(),
            privilegeTypesToPrivileges(privilegeTypes, user, State.DENY));
    }

    PrivilegesAnalyzedStatement analyzeRevoke(RevokePrivilege node, User user) {
        validateUsernames(node.userNames());
        Collection<Privilege.Type> privilegeTypes;
        if (node.all()) {
            privilegeTypes = Privilege.GRANTABLE_TYPES;
        } else {
            privilegeTypes = parsePrivilegeTypes(node.privileges());
        }
        return new PrivilegesAnalyzedStatement(node.userNames(),
            privilegeTypesToPrivileges(privilegeTypes, user, State.REVOKE));
    }

    private void validateUsernames(List<String> userNames) {
        for (String userName : userNames) {
            User user = userManager.findUser(userName);
            if (user == null) {
                throw new ResourceNotFoundException(String.format(
                    Locale.ENGLISH, "User %s does not exists", userName));
            } else if (user.roles().contains(User.Role.SUPERUSER)) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH, "Cannot alter privileges for superuser %s", userName));
            }
        }
    }

    private List<Privilege.Type> parsePrivilegeTypes(List<String> privilegeTypeNames) {
        List<Privilege.Type> privilegeTypes = new ArrayList<>(privilegeTypeNames.size());
        for (String typeName : privilegeTypeNames) {
            Privilege.Type privilegeType;
            try {
                privilegeType = Privilege.Type.valueOf(typeName.toUpperCase(Locale.ENGLISH));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Unknown privilege type '%s'", typeName));
            }
            if (Privilege.GRANTABLE_TYPES.contains(privilegeType) == false) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Unknown privilege type '%s'", typeName));
            }
            privilegeTypes.add(privilegeType);
        }
        return privilegeTypes;
    }

    private Set<Privilege> privilegeTypesToPrivileges(Collection<Privilege.Type> privilegeTypes,
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
