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

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.Id;
import io.crate.analyze.PrimaryKeyVisitor;
import io.crate.analyze.where.PartitionResolver;
import io.crate.analyze.where.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.BytesRefValueSymbolVisitor;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import org.apache.lucene.util.BytesRef;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class TableRelation extends AnalyzedRelation {

    private static final PrimaryKeyVisitor PK_VISITOR = new PrimaryKeyVisitor();
    private static final Predicate<ReferenceInfo> HAS_OBJECT_ARRAY_PARENT = new Predicate<ReferenceInfo>() {
        @Override
        public boolean apply(@Nullable ReferenceInfo input) {
            return input != null
                    && input.type().id() == ArrayType.ID
                    && ((ArrayType)input.type()).innerType().equals(DataTypes.OBJECT);
        }
    };

    private final TableInfo tableInfo;
    private final PartitionResolver partitionResolver;
    private ImmutableList<TableInfo> tables;
    private WhereClause whereClause;
    private List<String> ids = new ArrayList<>();
    private List<String> routingValues = new ArrayList<>();


    public TableRelation(TableInfo tableInfo, PartitionResolver partitionResolver) {
        super();
        this.tableInfo = tableInfo;
        this.partitionResolver = partitionResolver;
    }

    public TableInfo tableInfo() {
        return tableInfo;
    }

    public List<String> ids() {
        return ids;
    }

    public List<String> routingValues() {
        return routingValues;
    }

    @Override
    public List<AnalyzedRelation> children() {
        return ImmutableList.of();
    }

    @Override
    public int numRelations() {
        return 0;
    }

    @Override
    public WhereClause whereClause() {
        return whereClause;
    }

    @Override
    public void whereClause(WhereClause whereClause) {
        this.whereClause = whereClause;
        if (whereClause.hasQuery()) {
            PrimaryKeyVisitor.Context ctx = PK_VISITOR.process(tableInfo, whereClause.query());
            if (ctx != null) {
                whereClause.clusteredByLiteral(ctx.clusteredByLiteral());
                if (ctx.noMatch) {
                    this.whereClause = WhereClause.NO_MATCH;
                } else {
                    whereClause.version(ctx.version());
                    if (ctx.keyLiterals() != null) {
                        processPrimaryKeyLiterals(ctx.keyLiterals(), whereClause);
                    }
                }
            }

            if (tableInfo.isPartitioned()) {
                this.whereClause = partitionResolver.resolvePartitions(this.whereClause, tableInfo);
            }
        }
    }

    @Override
    public Reference getReference(@Nullable String schema,
                                  @Nullable String tableOrAlias,
                                  ColumnIdent columnIdent) {
        ReferenceInfo referenceInfo = tableInfo.getReferenceInfo(columnIdent);
        if (referenceInfo == null) {
            referenceInfo = tableInfo.getIndexReferenceInfo(columnIdent);
            if (referenceInfo == null) {
                return tableInfo.dynamicReference(columnIdent);
            }
        }

        // TODO: build type correctly as array when the tableInfo is created and remove the conversion here
        if (!columnIdent.isColumn() && hasMatchingParent(referenceInfo, HAS_OBJECT_ARRAY_PARENT)) {
            if (DataTypes.isCollectionType(referenceInfo.type())) {
                // TODO: remove this limitation with next type refactoring
                throw new UnsupportedOperationException(
                        "cannot query for arrays inside object arrays explicitly");
            }
            // for child fields of object arrays
            // return references of primitive types as array

            referenceInfo = new ReferenceInfo.Builder()
                    .ident(referenceInfo.ident())
                    .objectType(referenceInfo.objectType())
                    .granularity(referenceInfo.granularity())
                    .type(new ArrayType(referenceInfo.type()))
                    .build();
        }
        return new Reference(referenceInfo);
    }

    @Override
    public List<TableInfo> tables() {
        if (tables == null) {
            tables = ImmutableList.of(tableInfo);
        }
        return tables;
    }

    @Override
    public <C, R> R accept(RelationVisitor<C, R> relationVisitor, C context) {
        return relationVisitor.visitTableRelation(this, context);
    }

    @Override
    public boolean addressedBy(String relationName) {
        return tableInfo.ident().name().equals(relationName);
    }

    @Override
    public boolean addressedBy(@Nullable String schemaName, String tableName) {
        if (schemaName == null) {
            return addressedBy(tableName);
        }
        return tableInfo.schemaInfo().name().equals(schemaName) && addressedBy(tableName);
    }

    @Override
    public void normalize(EvaluatingNormalizer normalizer) {
        if (whereClause == null) {
            return;
        }
        if (whereClause.hasQuery()) {
            whereClause.normalize(normalizer);
        }
    }

    /**
     * return true if the given {@linkplain com.google.common.base.Predicate}
     * returns true for a parent column of this one.
     * returns false if info has no parent column.
     */
    private boolean hasMatchingParent(ReferenceInfo info,
                                      Predicate<ReferenceInfo> parentMatchPredicate) {
        ColumnIdent parent = info.ident().columnIdent().getParent();
        while (parent != null) {
            ReferenceInfo parentInfo = tableInfo.getReferenceInfo(parent);
            if (parentMatchPredicate.apply(parentInfo)) {
                return true;
            }
           parent = parent.getParent();
        }
        return false;
    }

    private void processPrimaryKeyLiterals(List primaryKeyLiterals, WhereClause whereClause) {
        List<List<BytesRef>> primaryKeyValuesList = new ArrayList<>(primaryKeyLiterals.size());
        primaryKeyValuesList.add(new ArrayList<BytesRef>(tableInfo.primaryKey().size()));

        for (int i=0; i<primaryKeyLiterals.size(); i++) {
            Object primaryKey = primaryKeyLiterals.get(i);
            if (primaryKey instanceof Literal) {
                Literal pkLiteral = (Literal)primaryKey;
                // support e.g. pk IN (Value..,)
                if (pkLiteral.valueType().id() == SetType.ID) {
                    Set<Literal> literals = Sets.newHashSet(Literal.explodeCollection(pkLiteral));
                    Iterator<Literal> literalIterator = literals.iterator();
                    for (int s=0; s<literals.size(); s++) {
                        Literal pk = literalIterator.next();
                        if (s >= primaryKeyValuesList.size()) {
                            // copy already parsed pk values, so we have all possible multiple pk for all sets
                            primaryKeyValuesList.add(Lists.newArrayList(primaryKeyValuesList.get(s - 1)));
                        }
                        List<BytesRef> primaryKeyValues = primaryKeyValuesList.get(s);
                        if (primaryKeyValues.size() > i) {
                            primaryKeyValues.set(i, BytesRefValueSymbolVisitor.INSTANCE.process(pk));
                        } else {
                            primaryKeyValues.add(BytesRefValueSymbolVisitor.INSTANCE.process(pk));
                        }
                    }
                } else {
                    for (List<BytesRef> primaryKeyValues : primaryKeyValuesList) {
                        primaryKeyValues.add((BytesRefValueSymbolVisitor.INSTANCE.process(pkLiteral)));
                    }
                }
            } else if (primaryKey instanceof List) {
                primaryKey = Lists.transform((List<Literal>) primaryKey, new com.google.common.base.Function<Literal, BytesRef>() {
                    @Override
                    public BytesRef apply(Literal input) {
                        return BytesRefValueSymbolVisitor.INSTANCE.process(input);
                    }
                });
                primaryKeyValuesList.add((List<BytesRef>) primaryKey);
            }
        }

        for (List<BytesRef> primaryKeyValues : primaryKeyValuesList) {
            addIdAndRouting(primaryKeyValues, whereClause.clusteredBy().orNull());
        }
    }

    private void addIdAndRouting(List<BytesRef> primaryKeyValues, String clusteredByValue) {
        ColumnIdent clusteredBy = tableInfo.clusteredBy();
        Id id = new Id(tableInfo.primaryKey(), primaryKeyValues, clusteredBy == null ? null : clusteredBy, false);
        if (id.isValid()) {
            String idString = id.stringValue();
            ids.add(idString);
            if (clusteredByValue == null) {
                clusteredByValue = idString;
            }
        }
        if (clusteredByValue != null) {
            routingValues.add(clusteredByValue);
        }
    }
}
