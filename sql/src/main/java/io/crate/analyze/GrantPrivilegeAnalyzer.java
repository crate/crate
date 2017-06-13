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

import io.crate.operation.user.UserManager;
import io.crate.sql.tree.GrantPrivilege;

import java.util.Locale;

public class GrantPrivilegeAnalyzer {

    private final UserManager userManager;

    public GrantPrivilegeAnalyzer(UserManager userManager) {
        this.userManager = userManager;
    }

    public GrantRevokePrivilegeAnalyzedStatement analyze(GrantPrivilege node) {
        for (String user : node.userNames()) {
            if (userManager.findUser(user) == null) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH, "User %s does not exists", user));
            }
        }
        return new GrantRevokePrivilegeAnalyzedStatement(node.userNames(), node.privileges());
    }
}
