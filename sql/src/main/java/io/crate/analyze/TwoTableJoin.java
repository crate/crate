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

import com.google.common.base.Optional;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.Field;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.ColumnIndex;
import io.crate.metadata.Path;
import io.crate.metadata.table.Operation;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.sql.tree.QualifiedName;

import java.util.ArrayList;
import java.util.List;

public class TwoTableJoin implements QueriedRelation {

    private final QuerySpec querySpec;
    private final QualifiedName leftName;
    private final MultiSourceSelect.Source left;
    private final QualifiedName rightName;
    private final MultiSourceSelect.Source right;
    private final Optional<OrderBy> remainingOrderBy;
    private final List<Field> fields;
    private final QualifiedName name;
    private final JoinType joinType;

    public TwoTableJoin(QuerySpec querySpec,
                        QualifiedName leftName,
                        MultiSourceSelect.Source left,
                        QualifiedName rightName,
                        MultiSourceSelect.Source right,
                        Optional<OrderBy> remainingOrderBy,
                        JoinType joinType) {
        this.querySpec = querySpec;
        this.leftName = leftName;
        this.left = left;
        this.rightName = rightName;
        this.right = right;
        this.name = QualifiedName.of("join", leftName.toString(), rightName.toString());
        this.remainingOrderBy = remainingOrderBy;
        this.joinType = joinType;
        fields = new ArrayList<>(querySpec.outputs().size());
        for (int i = 0; i < querySpec.outputs().size(); i++) {
            fields.add(new Field(this, new ColumnIndex(i), querySpec.outputs().get(i).valueType()));
        }
    }

    public Optional<OrderBy> remainingOrderBy() {
        return remainingOrderBy;
    }

    public MultiSourceSelect.Source left() {
        return left;
    }

    public MultiSourceSelect.Source right() {
        return right;
    }

    @Override
    public QuerySpec querySpec() {
        return querySpec;
    }

    public JoinType joinType() {
        return joinType;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitTwoTableJoin(this, context);
    }

    @Override
    public Field getField(Path path, Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
        throw new UnsupportedOperationException("getField is not supported");
    }

    @Override
    public List<Field> fields() {
        return fields;
    }

    @Override
    public QualifiedName getQualifiedName() {
        throw new UnsupportedOperationException("method not supported");
    }

    @Override
    public void setQualifiedName(QualifiedName qualifiedName) {
        throw new UnsupportedOperationException("method not supported");
    }

    public QualifiedName leftName() {
        return leftName;
    }

    public QualifiedName rightName() {
        return rightName;
    }

    public QualifiedName name() {
        return name;
    }

    @Override
    public String toString() {
        return name.toString();
    }
}
