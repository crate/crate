/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.util.Optional;

public class ShowTables extends Statement {

    private final Optional<QualifiedName> schema;
    private final Optional<String> likePattern;
    private final Optional<Expression> whereExpression;

    public ShowTables(Optional<QualifiedName> schema, Optional<String> likePattern, Optional<Expression> whereExpression) {
        this.schema = schema;
        this.whereExpression = whereExpression;
        this.likePattern = likePattern;
    }

    public Optional<QualifiedName> schema() {
        return schema;
    }

    public Optional<String> likePattern() {
        return likePattern;
    }

    public Optional<Expression> whereExpression() {
        return whereExpression;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowTables(this, context);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(schema, whereExpression, likePattern);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ShowTables o = (ShowTables) obj;
        return Objects.equal(schema, o.schema) &&
            Objects.equal(likePattern, o.likePattern) &&
            Objects.equal(whereExpression, o.whereExpression);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("schema", schema)
            .add("likePattern", likePattern.toString())
            .add("whereExpression", whereExpression.toString())
            .toString();
    }
    @Override
    public PrivilegeType privilegeType() {
        return PrivilegeType.DQL;
    }

    @Override
    public PrivilegeClazz privilegeClazz() {
        return PrivilegeClazz.SCHEMA;
    }

    @Override
    public String privilegeIdent() {
        return schema.toString();
    }
}
