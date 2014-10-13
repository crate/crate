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
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.where.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.Reference;
import javax.annotation.Nullable;
import java.util.List;

public class AliasedAnalyzedRelation extends AnalyzedRelation {

    private final String alias;
    private final AnalyzedRelation child;
    private ImmutableList<AnalyzedRelation> children;

    public AliasedAnalyzedRelation(String alias, AnalyzedRelation child) {
        this.alias = alias;
        this.child = child;
    }

    @Override
    public List<AnalyzedRelation> children() {
        if (children == null) {
            children = ImmutableList.of(child);
        }
        return children;
    }

    public String alias() {
        return alias;
    }

    @Override
    public int numRelations() {
        return child.numRelations();
    }

    @Override
    public WhereClause whereClause() {
        return child.whereClause();
    }

    @Override
    public void whereClause(WhereClause whereClause) {
        child.whereClause(whereClause);
    }

    @Override
    public Reference getReference(@Nullable String schema, @Nullable String tableOrAlias, ColumnIdent columnIdent) {
        // TODO: check tableOrAlias...
        return child.getReference(schema, tableOrAlias, columnIdent);
    }

    @Override
    public List<TableInfo> tables() {
        return child.tables();
    }

    @Override
    public <C, R> R accept(RelationVisitor<C, R> relationVisitor, C context) {
        return relationVisitor.visitAliasedRelation(this, context);
    }

    @Override
    public boolean addressedBy(String relationName) {
        return alias.equals(relationName);
    }

    @Override
    public boolean addressedBy(@Nullable String schemaName, String tableName) {
        if (schemaName != null) {
            return false;
        }
        return addressedBy(tableName);
    }

    @Override
    public void normalize(EvaluatingNormalizer normalizer) {
        child.normalize(normalizer);
    }
}
