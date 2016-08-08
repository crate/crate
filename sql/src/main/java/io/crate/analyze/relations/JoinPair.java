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

package io.crate.analyze.relations;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import io.crate.analyze.symbol.Symbol;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;

public class JoinPair {

    private final JoinType joinType;

    private QualifiedName left;
    private QualifiedName right;
    @Nullable
    private Symbol condition;

    public JoinPair(QualifiedName left, QualifiedName right, JoinType joinType) {
        this(left, right, joinType, null);
    }

    public JoinPair(QualifiedName left, QualifiedName right, JoinType joinType, @Nullable Symbol condition) {
        this.left = left;
        this.right = right;
        this.joinType = joinType;
        this.condition = condition;
    }

    public QualifiedName left() {
        return left;
    }

    public QualifiedName right() {
        return right;
    }

    public JoinType joinType() {
        return joinType;
    }

    @Nullable
    public Symbol condition() {
        return condition;
    }

    public void condition(Symbol condition) {
        this.condition = condition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JoinPair joinPair = (JoinPair) o;
        return Objects.equal(left, joinPair.left) &&
               Objects.equal(right, joinPair.right) &&
               joinType == joinPair.joinType &&
               Objects.equal(condition, joinPair.condition);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(left, right, joinType, condition);
    }

    public void replaceCondition(Function<? super Symbol, Symbol> replaceFunction) {
        condition = replaceFunction.apply(condition);
    }

    boolean equalsNames(QualifiedName left, QualifiedName right) {
        return this.left.equals(left) && this.right.equals(right);
    }

    void replaceNames(QualifiedName left, QualifiedName right, QualifiedName newName) {
        if (this.left.equals(left) || this.left.equals(right)) {
            this.left = newName;
        }
        if (this.right.equals(right) || this.right.equals(left)) {
            this.right = newName;
        }
    }

    boolean isOuterRelation(QualifiedName name) {
        if (joinType.isOuter()) {
            if (left.equals(name) && (joinType == JoinType.RIGHT || joinType == JoinType.FULL)) {
                return true;
            }
            if (right.equals(name) && (joinType == JoinType.LEFT || joinType == JoinType.FULL)) {
                return true;
            }
        }
        return false;
    }
}
