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

package io.crate.metadata.relation;

import com.google.common.collect.ImmutableList;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexReferenceInfo;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.DynamicReference;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class JoinRelation implements AnalyzedRelation {

    private final Type type;
    private final AnalyzedRelation left;
    private final AnalyzedRelation right;
    private final List<AnalyzedRelation> children;
    private final List<TableInfo> tables;

    public enum Type {
        CROSS_JOIN
    }

    public JoinRelation(Type type, AnalyzedRelation left, AnalyzedRelation right) {
        this.type = type;
        this.left = left;
        this.right = right;
        this.children = ImmutableList.of(left, right);
        this.tables = new ArrayList<>(left.tables());
        this.tables.addAll(right.tables());
    }

    public Type type() {
        return type;
    }

    public AnalyzedRelation left() {
        return left;
    }

    public AnalyzedRelation right() {
        return right;
    }

    @Override
    public List<AnalyzedRelation> children() {
        return children;
    }

    @Override
    public int numRelations() {
        return 3;
    }

    @Nullable
    @Override
    public ReferenceInfo getReferenceInfo(ColumnIdent columnIdent) {
        ReferenceInfo leftInfo = left.getReferenceInfo(columnIdent);
        if (leftInfo == null) {
            return right.getReferenceInfo(columnIdent);
        } else {
            ReferenceInfo rightInfo = right.getReferenceInfo(columnIdent);
            if (rightInfo == null) {
                return leftInfo;
            }
        }

        throw new UnsupportedOperationException(String.format(
                "column \"%s\" is ambiguous and exists in both \"%s\" and \"%s\"",
                columnIdent.sqlFqn(),
                left,
                right)
        );
    }

    @Nullable
    @Override
    public IndexReferenceInfo getIndexReferenceInfo(ColumnIdent columnIdent) {
        return null;
    }

    @Override
    public List<TableInfo> tables() {
        return tables;
    }

    @Override
    public <C, R> R accept(RelationVisitor<C, R> relationVisitor, C context) {
        return relationVisitor.visitCrossJoinRelation(this, context);
    }

    @Override
    public boolean addressedBy(String relationName) {
        return left.addressedBy(relationName) || right.addressedBy(relationName);
    }

    @Override
    public boolean addressedBy(@Nullable String schemaName, String tableName) {
        return left.addressedBy(schemaName, tableName) || right.addressedBy(schemaName, tableName);
    }

    @Override
    public DynamicReference dynamicReference(ColumnIdent columnIdent) throws ColumnUnknownException {
        // dynamicReference is only called if neither left nor right knew the table
        // -> means it's not possible to determine to which source the columnIdent belongs to...
        throw new ColumnUnknownException(columnIdent.sqlFqn());
    }
}
