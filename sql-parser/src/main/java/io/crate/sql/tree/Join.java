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

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class Join extends Relation {
    public Join(Type type, Relation left, Relation right, @Nullable JoinCriteria criteria) {
        checkNotNull(left, "left is null");
        checkNotNull(right, "right is null");
        if (type.equals(Type.CROSS)) {
            checkArgument(criteria == null, "Cross join cannot have join criteria");
        } else {
            checkNotNull(criteria, "No join criteria specified");
        }

        this.type = type;
        this.left = left;
        this.right = right;
        this.criteria = criteria;
    }

    public enum Type {
        CROSS, INNER, LEFT, RIGHT, FULL
    }

    private final Type type;
    private final Relation left;
    private final Relation right;
    @Nullable
    private final JoinCriteria criteria;

    public Type getType() {
        return type;
    }

    public Relation getLeft() {
        return left;
    }

    public Relation getRight() {
        return right;
    }

    @Nullable
    public JoinCriteria getCriteria() {
        return criteria;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitJoin(this, context);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("type", type)
            .add("left", left)
            .add("right", right)
            .add("criteria", criteria)
            .omitNullValues()
            .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Join join = (Join) o;

        if (criteria != null ? !criteria.equals(join.criteria) : join.criteria != null) {
            return false;
        }
        if (!left.equals(join.left)) {
            return false;
        }
        if (!right.equals(join.right)) {
            return false;
        }
        if (type != join.type) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + left.hashCode();
        result = 31 * result + right.hashCode();
        result = 31 * result + (criteria != null ? criteria.hashCode() : 0);
        return result;
    }
}
