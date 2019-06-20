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

import com.google.common.collect.ImmutableList;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

public class CopyFromReturnSummaryAnalyzedStatement extends CopyFromAnalyzedStatement implements AnalyzedRelation {

    private final List<Field> fields = ImmutableList.of(
        new Field(this, new ColumnIdent("node"), new InputColumn(0, ObjectType.builder()
            .setInnerType("id", DataTypes.STRING)
            .setInnerType("name", DataTypes.STRING)
            .build())),
        new Field(this, new ColumnIdent("uri"), new InputColumn(1, DataTypes.STRING)),
        new Field(this, new ColumnIdent("success_count"), new InputColumn(2, DataTypes.LONG)),
        new Field(this, new ColumnIdent("error_count"), new InputColumn(3, DataTypes.LONG)),
        new Field(this, new ColumnIdent("errors"), new InputColumn(4, ObjectType.untyped()))
    );

    private final QualifiedName qualifiedName;

    CopyFromReturnSummaryAnalyzedStatement(DocTableInfo tableInfo,
                                           Settings settings,
                                           Symbol uri,
                                           @Nullable String partitionIdent,
                                           Predicate<DiscoveryNode> nodePredicate,
                                           FileUriCollectPhase.InputFormat inputFormat) {
        super(tableInfo, settings, uri, partitionIdent, nodePredicate, inputFormat);
        qualifiedName = new QualifiedName(Arrays.asList(tableInfo.ident().schema(), tableInfo.ident().name()));
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        throw new UnsupportedOperationException(
            getClass().getCanonicalName() + " is virtual relation, visiting it is unsupported");
    }

    @Override
    public Field getField(ColumnIdent path, Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
        throw new UnsupportedOperationException("getField is unsupported on internal relation for copy from return");
    }

    @Override
    public List<Field> fields() {
        return fields;
    }

    @Override
    public QualifiedName getQualifiedName() {
        return qualifiedName;
    }

    @Override
    public List<Symbol> outputs() {
        return List.copyOf(fields);
    }

    @Override
    public WhereClause where() {
        return WhereClause.MATCH_ALL;
    }

    @Override
    public List<Symbol> groupBy() {
        return List.of();
    }

    @Nullable
    @Override
    public HavingClause having() {
        return null;
    }

    @Nullable
    @Override
    public OrderBy orderBy() {
        return null;
    }

    @Nullable
    @Override
    public Symbol limit() {
        return null;
    }

    @Nullable
    @Override
    public Symbol offset() {
        return null;
    }

    @Override
    public boolean hasAggregates() {
        return false;
    }

    @Override
    public boolean isDistinct() {
        return false;
    }
}
