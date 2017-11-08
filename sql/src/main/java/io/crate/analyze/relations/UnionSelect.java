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

import io.crate.analyze.Fields;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.symbol.Field;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.Path;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

public class UnionSelect implements QueriedRelation {

    private final QuerySpec querySpec = new QuerySpec();
    private final Fields fields;
    private QueriedRelation left;
    private QueriedRelation right;
    private QualifiedName name;

    public UnionSelect(QueriedRelation left, QueriedRelation right) {
        this.left = left;
        this.right = right;
        this.name = left.getQualifiedName();

        List<Field> fieldsFromLeft = left.fields();
        fields = new Fields(fieldsFromLeft.size());
        for (Field field : fieldsFromLeft) {
            fields.add(field.path(), new Field(this, field.path(), field.valueType()));
        }
        querySpec.outputs(new ArrayList<>(fields.asList()));
    }

    public QueriedRelation left() {
        return left;
    }

    public QueriedRelation right() {
        return right;
    }

    public void left(QueriedRelation left) {
        this.left = left;
    }

    public void right(QueriedRelation right) {
        this.right = right;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitUnionSelect(this, context);
    }

    @Override
    public Field getField(Path path, Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
        if (operation != Operation.READ) {
            throw new UnsupportedOperationException("getField on MultiSourceSelect is only supported for READ operations");
        }
        return fields.get(path);
    }

    @Override
    public List<Field> fields() {
        return fields.asList();
    }

    @Override
    public QualifiedName getQualifiedName() {
        return name;
    }

    @Override
    public void setQualifiedName(@Nonnull QualifiedName qualifiedName) {
        name = qualifiedName;
    }

    @Override
    public QuerySpec querySpec() {
        return querySpec;
    }

    @Override
    public String toString() {
        return "US{" + left.getQualifiedName().toString() + ',' + right.getQualifiedName().toString() + '}';
    }
}
