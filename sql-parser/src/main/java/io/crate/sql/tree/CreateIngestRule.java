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

import javax.annotation.Nullable;


public class CreateIngestRule extends Statement {

    private final String rule;
    private final String source;
    private final QualifiedName target;
    @Nullable
    private final Expression where;

    public CreateIngestRule(String rule,
                            String sourceIdent,
                            QualifiedName targetTable,
                            @Nullable Expression where) {
        this.rule = rule;
        this.source = sourceIdent;
        this.target = targetTable;
        this.where = where;
    }

    public String ruleName() {
        return rule;
    }

    public String sourceIdent() {
        return source;
    }

    public QualifiedName targetTable() {
        return target;
    }

    @Nullable
    public Expression where() {
        return where;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateIngestRule(this, context);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(rule, source, target, where);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("rule", rule)
            .add("source", source)
            .add("target", target)
            .add("where", where)
            .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CreateIngestRule other = (CreateIngestRule) o;

        if (rule != null ? !rule.equals(other.rule) : other.rule != null) return false;
        if (source != null ? !source.equals(other.source) : other.source != null) return false;
        if (target != null ? !target.equals(other.target) : other.target != null) return false;
        if (where != null ? !where.equals(other.where) : other.where != null) return false;

        return true;
    }
}
