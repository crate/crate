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

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.where.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;

import javax.annotation.Nullable;
import java.util.List;

/**
 * <h2>Represents an analyzed query specification</h2>
 *
 * <code>
 * select &lt;outputs&gt; from &lt;sourceRelation&gt; <br />
 *      where &lt;whereClause&gt; <br />
 *      group by &lt;groupBy&gt;  <br />
 *      having &lt;having&gt; <br />
 *      order by &lt;orderBy&gt; <br />
 *      limit &lt;limit&gt; offset &lt;offset&gt;
 * </code>
 */
public class AnalyzedQuerySpecification implements AnalyzedRelation {

    public static final boolean[] EMPTY_REVERSE_FLAGS = new boolean[0];
    public static final Boolean[] EMPTY_NULLS_FIRST = new Boolean[0];

    private final AnalyzedRelation sourceRelation;
    private final List<Symbol> outputs;
    private final List<Symbol> groupBy;
    private final Optional<Symbol> having;
    private final List<Symbol> orderBy;
    private final boolean[] reverseFlags;
    private final Boolean[] nullsFirst;
    private final Integer offset;
    private final Integer limit;
    private ImmutableList<AnalyzedRelation> children;

    public AnalyzedQuerySpecification(List<Symbol> outputs,
                                      AnalyzedRelation sourceRelation,
                                      @Nullable List<Symbol> groupBy,
                                      @Nullable Symbol having,
                                      @Nullable List<Symbol> orderBy,
                                      @Nullable boolean[] reverseFlags,
                                      @Nullable Boolean[] nullsFirst,
                                      @Nullable Integer limit,
                                      @Nullable Integer offset) {
        this.outputs = outputs;
        this.sourceRelation = sourceRelation;
        this.groupBy = Objects.firstNonNull(groupBy, ImmutableList.<Symbol>of());
        this.having = Optional.fromNullable(having);
        this.orderBy = Objects.firstNonNull(orderBy, ImmutableList.<Symbol>of());
        this.reverseFlags = Objects.firstNonNull(reverseFlags, EMPTY_REVERSE_FLAGS);
        this.nullsFirst = Objects.firstNonNull(nullsFirst, EMPTY_NULLS_FIRST);
        this.limit = limit;
        this.offset = Objects.firstNonNull(offset, 0);
    }

    public List<Symbol> outputs() {
        return outputs;
    }

    public AnalyzedRelation sourceRelation() {
        return sourceRelation;
    }

    public WhereClause whereClause() {
        return sourceRelation.whereClause();
    }

    @Override
    public void whereClause(WhereClause whereClause) {
        sourceRelation.whereClause(whereClause);
    }

    @Override
    public Reference getReference(@Nullable String schema,
                                  @Nullable String tableOrAlias,
                                  ColumnIdent columnIdent,
                                  boolean forWrite) {
        return sourceRelation.getReference(schema, tableOrAlias, columnIdent, forWrite);
    }

    public List<Symbol> groupBy() {
        return groupBy;
    }

    public boolean hasGroupBy() {
        return groupBy.size() > 0;
    }

    public Optional<Symbol> having() {
        return having;
    }

    public List<Symbol> orderBy() {
        return orderBy;
    }

    public boolean[] reverseFlags() {
        return reverseFlags;
    }

    public Boolean[] nullsFirst() {
        return nullsFirst;
    }

    @Nullable
    public Integer limit() {
        return limit;
    }

    public int offset() {
        return offset;
    }

    @Override
    public List<AnalyzedRelation> children() {
        if (children == null) {
            children = ImmutableList.of(sourceRelation);
        }
        return children;
    }

    @Override
    public int numRelations() {
        return 1;
    }

    @Override
    public List<TableInfo> tables() {
        return sourceRelation.tables();
    }

    @Override
    public <C, R> R accept(RelationVisitor<C, R> relationVisitor, C context) {
        return relationVisitor.visitQuerySpecification(this, context);
    }

    @Override
    public boolean addressedBy(String relationName) {
        return sourceRelation.addressedBy(relationName);
    }

    @Override
    public boolean addressedBy(@Nullable String schemaName, String tableName) {
        return sourceRelation.addressedBy(schemaName, tableName);
    }

    @Override
    public void normalize(EvaluatingNormalizer normalizer) {
        sourceRelation.normalize(normalizer);
        normalizer.normalizeInplace(outputs);
        normalizer.normalizeInplace(groupBy);
        normalizer.normalizeInplace(orderBy);
    }
}
