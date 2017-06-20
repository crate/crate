/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.List;

public class SetStatement extends Statement {

    public enum Scope {
        GLOBAL, SESSION, LOCAL
    }

    public enum SettingType {
        TRANSIENT, PERSISTENT
    }

    private final Scope scope;
    private final SettingType settingType;
    private final List<Assignment> assignments;

    public SetStatement(Scope scope, List<Assignment> assignments) {
        this(scope, SettingType.TRANSIENT, assignments);
    }

    public SetStatement(Scope scope, SettingType settingType, List<Assignment> assignments) {
        Preconditions.checkNotNull(assignments, "assignments are null");
        this.scope = scope;
        this.settingType = settingType;
        this.assignments = assignments;
    }

    public SetStatement(Scope scope, Assignment assignment) {
        Preconditions.checkNotNull(assignment, "assignment is null");
        this.scope = scope;
        this.settingType = SettingType.TRANSIENT;
        this.assignments = Collections.singletonList(assignment);
    }

    public Scope scope() {
        return scope;
    }

    public List<Assignment> assignments() {
        return assignments;
    }

    public SettingType settingType() {
        return settingType;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(scope, assignments, settingType);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("scope", scope)
            .add("assignments", assignments)
            .add("settingType", settingType)
            .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SetStatement update = (SetStatement) o;

        if (!scope.equals(update.scope)) return false;
        if (!assignments.equals(update.assignments)) return false;
        if (!settingType.equals(update.settingType)) return false;

        return true;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSetStatement(this, context);
    }

    @Override
    public PrivilegeType privilegeType() {
        if (Scope.GLOBAL.equals(scope)) {
            return PrivilegeType.DCL;
        }
        return PrivilegeType.DQL;
    }

    @Override
    public String clazz() { return "CLUSTER";}
}
