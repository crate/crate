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

package io.crate.sql.tree;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public abstract sealed class PrivilegeStatement extends Statement
    permits GrantPrivilege, RevokePrivilege, DenyPrivilege {

    protected final List<String> userNames;
    protected final List<String> permissions;
    private final List<QualifiedName> tableOrSchemaNames;
    private final String securable;
    protected final boolean all;

    protected PrivilegeStatement(List<String> userNames, String securable, List<QualifiedName> tableOrSchemaNames) {
        this.userNames = userNames;
        permissions = Collections.emptyList();
        all = true;
        this.securable = securable;
        this.tableOrSchemaNames = tableOrSchemaNames;
    }

    protected PrivilegeStatement(List<String> userNames, List<String> permissions, String securable, List<QualifiedName> tableOrSchemaNames) {
        this.userNames = userNames;
        this.permissions = permissions;
        this.all = false;
        this.securable = securable;
        this.tableOrSchemaNames = tableOrSchemaNames;
    }

    public List<String> privileges() {
        return permissions;
    }

    public List<String> userNames() {
        return userNames;
    }

    public boolean all() {
        return all;
    }

    public List<QualifiedName> privilegeIdents() {
        return tableOrSchemaNames;
    }

    public String securable() {
        return securable;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof PrivilegeStatement that
            && all == that.all
            && Objects.equals(userNames, that.userNames)
            && Objects.equals(permissions, that.permissions)
            && Objects.equals(securable, that.securable)
            && Objects.equals(tableOrSchemaNames, that.tableOrSchemaNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(all, userNames, permissions, securable, tableOrSchemaNames);
    }

}
