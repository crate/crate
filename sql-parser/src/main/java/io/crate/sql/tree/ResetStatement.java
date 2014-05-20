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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.util.List;

public class ResetStatement extends Statement {

    private final SetStatement.SettingType settingType;
    private final List<Expression> columns;

    public ResetStatement(List<Expression> columns) {
        this(SetStatement.SettingType.TRANSIENT, columns);
    }

    public ResetStatement(SetStatement.SettingType settingType, List<Expression> columns) {
        Preconditions.checkNotNull(columns, "columns are null");
        this.settingType = settingType;
        this.columns = columns;
    }


    public List<Expression> columns() {
        return columns;
    }

    public SetStatement.SettingType settingType() {
        return settingType;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(columns, settingType);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("columns", columns)
                .add("settingType", settingType)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ResetStatement update = (ResetStatement) o;

        if (!columns.equals(update.columns)) return false;
        if (!settingType.equals(update.settingType)) return false;

        return true;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitResetStatement(this, context);
    }
}
