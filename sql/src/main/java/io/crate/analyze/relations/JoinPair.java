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

import com.google.common.base.Objects;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class JoinPair {

    private QualifiedName left;
    private QualifiedName right;
    private final JoinType joinType;

    public JoinPair(QualifiedName left, QualifiedName right, JoinType joinType) {
        this.left = left;
        this.right = right;
        this.joinType = joinType;
    }

    public boolean equalsNames(QualifiedName left, QualifiedName right) {
        return this.left.equals(left) && this.right.equals(right);
    }

    public void replaceNames(QualifiedName left, QualifiedName right, QualifiedName newName) {
        if (this.left.equals(left) || this.left.equals(right)) {
            this.left = newName;
        }
        if (this.right.equals(right) || this.right.equals(left)) {
            this.right = newName;
        }
    }

    public JoinType joinType() {
        return joinType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JoinPair that = (JoinPair) o;
        return Objects.equal(left, that.left) &&
               Objects.equal(right, that.right) &&
               joinType == that.joinType;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(left, right, joinType);
    }

    @Nullable
    public static JoinType joinTypeForRelations(QualifiedName left, QualifiedName right, List<JoinPair> joinPairs) {
        return joinTypeForRelations(left, right, joinPairs, true);
    }

    @Nullable
    public static JoinType joinTypeForRelations(QualifiedName left,
                                                QualifiedName right,
                                                List<JoinPair> joinPairs,
                                                boolean checkReversePair) {
        for (JoinPair joinPair : joinPairs) {
            if (joinPair.equalsNames(left, right)) {
                return joinPair.joinType();
            }
        }
        // check if relations were switched due to some optimization
        if (checkReversePair) {
            for (JoinPair joinPair : joinPairs) {
                if (joinPair.equalsNames(right, left)) {
                    return joinPair.joinType().invert();
                }
            }
        }

        return null;
    }

    public static Set<QualifiedName> outerJoinRelations(List<JoinPair> joinPairs) {
        Set<QualifiedName> outerJoinRelations = new HashSet<>();
        for (JoinPair joinPair : joinPairs) {
            if (joinPair.joinType().isOuter()) {
                outerJoinRelations.add(joinPair.left);
                outerJoinRelations.add(joinPair.right);
            }
        }
        return outerJoinRelations;
    }
}
