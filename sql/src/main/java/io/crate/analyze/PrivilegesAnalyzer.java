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

import io.crate.action.sql.SessionContext;
import io.crate.analyze.user.Privilege;
import io.crate.analyze.user.Privilege.State;
import io.crate.operation.user.User;
import io.crate.operation.user.UserManager;
import io.crate.sql.tree.GrantPrivilege;
import io.crate.sql.tree.PrivilegeType;
import io.crate.sql.tree.RevokePrivilege;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Analyzer for privileges related statements (ie GRANT/REVOKE statements)
 */
public class PrivilegesAnalyzer {

    private final UserManager userManager;

    public PrivilegesAnalyzer(UserManager userManager) {
        this.userManager = userManager;
    }

    public PrivilegesAnalyzedStatement analyzeGrant(GrantPrivilege node, SessionContext sessionContext) {
        validateUsernames(node.userNames());
        return new PrivilegesAnalyzedStatement(node.userNames(),
            privilegeTypesToPrivileges(node.privileges(), sessionContext.user(), true));
    }

    public PrivilegesAnalyzedStatement analyzeRevoke(RevokePrivilege node, SessionContext sessionContext) {
        validateUsernames(node.userNames());
        return new PrivilegesAnalyzedStatement(node.userNames(),
            privilegeTypesToPrivileges(node.privileges(), sessionContext.user(), false));
    }

    private void validateUsernames(List<String> userNames) {
        for (String userName : userNames) {
            User user = userManager.findUser(userName);
            if (user == null) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH, "User %s does not exists", userName));
            } else if (user.roles().contains(User.Role.SUPERUSER)) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH, "Cannot alter privileges for superuser %s", userName));
            }
        }
    }

    private Set<Privilege> privilegeTypesToPrivileges(EnumSet<PrivilegeType> privilegeTypes,
                                                      User grantor,
                                                      boolean grant) {
        Set<Privilege> privileges = new HashSet<>(privilegeTypes.size());
        for (PrivilegeType type : privilegeTypes) {
            State state = State.REVOKE;
            if (grant) {
                state = State.GRANT;
            }
            Privilege privilege = new Privilege(state,
                Privilege.Type.valueOf(type.name()),
                Privilege.Clazz.CLUSTER,
                null,
                grantor.name()
            );
            privileges.add(privilege);
        }
        return privileges;
    }
}
