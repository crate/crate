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

import com.google.common.collect.ImmutableList;
import io.crate.analyze.*;
import io.crate.analyze.symbol.Aggregations;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.format.SymbolPrinter;
import io.crate.metadata.OutputName;
import io.crate.metadata.Path;
import io.crate.metadata.ReplaceMode;
import io.crate.metadata.ReplacingSymbolVisitor;
import io.crate.operation.operator.AndOperator;
import io.crate.planner.Limits;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;

public final class SubselectRewriter {

    private final static Visitor INSTANCE = new Visitor();

    private SubselectRewriter() {
    }

    public static AnalyzedRelation rewrite(AnalyzedRelation relation) {
        return INSTANCE.process(relation, null);
    }

    private final static class Visitor extends AnalyzedRelationVisitor<QueriedSelectRelation, AnalyzedRelation> {

        @Override
        protected AnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, QueriedSelectRelation parent) {
            return relation;
        }

        @Override
        public AnalyzedRelation visitQueriedSelectRelation(QueriedSelectRelation relation, QueriedSelectRelation parent) {
            boolean mergedWithParent = false;
            QuerySpec currentQS = relation.querySpec();
            if (parent != null) {
                FieldReplacer fieldReplacer = new FieldReplacer(currentQS.outputs());
                QuerySpec parentQS = parent.querySpec().copyAndReplace(fieldReplacer);
                if (canBeMerged(currentQS, parentQS)) {
                    QuerySpec currentWithParentMerged = mergeQuerySpec(currentQS, parentQS);
                    relation = new QueriedSelectRelation(
                        relation.subRelation(),
                        namesFromOutputs(currentWithParentMerged.outputs(), fieldReplacer.replacedFieldsByNewOutput),
                        currentWithParentMerged
                    );
                    mergedWithParent = true;
                }
            }
            AnalyzedRelation origSubRelation = relation.subRelation();
            QueriedRelation subRelation = (QueriedRelation) process(origSubRelation, relation);

            if (origSubRelation == subRelation) {
                return relation;
            }
            if (!mergedWithParent && parent != null) {
                parent.subRelation(subRelation);
                FieldReplacer fieldReplacer = new FieldReplacer(subRelation.fields());
                parent.querySpec().replace(fieldReplacer);
            }
            if (relation.subRelation() != origSubRelation) {
                return relation;
            }

            return subRelation;
        }

        @Override
        public AnalyzedRelation visitQueriedTable(QueriedTable table, QueriedSelectRelation parent) {
            if (parent == null) {
                return table;
            }
            QuerySpec currentQS = table.querySpec();
            FieldReplacer fieldReplacer = new FieldReplacer(currentQS.outputs());
            QuerySpec parentQS = parent.querySpec().copyAndReplace(fieldReplacer);
            if (canBeMerged(currentQS, parentQS)) {
                QuerySpec currentWithParentMerged = mergeQuerySpec(currentQS, parentQS);
                return new QueriedTable(
                    table.tableRelation(),
                    namesFromOutputs(currentWithParentMerged.outputs(), fieldReplacer.replacedFieldsByNewOutput),
                    currentWithParentMerged
                );
            }
            return table;
        }

        @Override
        public AnalyzedRelation visitQueriedDocTable(QueriedDocTable table, QueriedSelectRelation parent) {
            if (parent == null) {
                return table;
            }
            QuerySpec currentQS = table.querySpec();
            FieldReplacer fieldReplacer = new FieldReplacer(currentQS.outputs());
            QuerySpec parentQS = parent.querySpec().copyAndReplace(fieldReplacer);
            if (canBeMerged(currentQS, parentQS)) {
                QuerySpec currentWithParentMerged = mergeQuerySpec(currentQS, parentQS);
                return new QueriedDocTable(
                    table.tableRelation(),
                    namesFromOutputs(currentWithParentMerged.outputs(), fieldReplacer.replacedFieldsByNewOutput),
                    currentWithParentMerged
                );
            }
            return table;
        }

        @Override
        public AnalyzedRelation visitMultiSourceSelect(MultiSourceSelect multiSourceSelect, QueriedSelectRelation parent) {
            if (parent == null) {
                return multiSourceSelect;
            }
            QuerySpec currentQS = multiSourceSelect.querySpec();
            FieldReplacer fieldReplacer = new FieldReplacer(currentQS.outputs());
            QuerySpec parentQS = parent.querySpec().copyAndReplace(fieldReplacer);
            if (canBeMerged(currentQS, parentQS)) {
                QuerySpec currentWithParentMerged = mergeQuerySpec(currentQS, parentQS);
                return new MultiSourceSelect(
                    multiSourceSelect.sources(),
                    namesFromOutputs(currentWithParentMerged.outputs(), fieldReplacer.replacedFieldsByNewOutput),
                    currentWithParentMerged,
                    multiSourceSelect.joinPairs()
                );
            }
            return multiSourceSelect;
        }
    }

    /**
     * @return new output names of a relation which has been merged with it's parent.
     *         It tries to preserve the alias/name of the parent if possible
     */
    private static Collection<Path> namesFromOutputs(Collection<? extends Symbol> outputs,
                                                     Map<Symbol, Field> replacedFieldsByNewOutput) {
        List<Path> outputNames = new ArrayList<>(outputs.size());
        for (Symbol output : outputs) {
            Field field = replacedFieldsByNewOutput.get(output);
            if (field == null) {
                if (output instanceof Path) {
                    outputNames.add((Path) output);
                } else {
                    outputNames.add(new OutputName(SymbolPrinter.INSTANCE.printSimple(output)));
                }
            } else {
                outputNames.add(field);
            }
        }
        return outputNames;
    }

    private static QuerySpec mergeQuerySpec(QuerySpec childQSpec, QuerySpec parentQSpec) {
        // merge everything: validation that merge is possible has already been done.
        OrderBy newOrderBy;
        if (parentQSpec.hasAggregates() || parentQSpec.groupBy().isPresent()) {
            // select avg(x) from (select x from t order by x)
            // -> can't keep order, but it doesn't matter for aggregations anyway so
            //    only keep the one from the parent Qspec
            newOrderBy = parentQSpec.orderBy().orElse(null);
        } else {
            newOrderBy = tryReplace(childQSpec.orderBy(), parentQSpec.orderBy());
        }
        return new QuerySpec()
            .outputs(parentQSpec.outputs())
            .where(mergeWhere(childQSpec.where(), parentQSpec.where()))
            .orderBy(newOrderBy)
            .offset(Limits.mergeAdd(childQSpec.offset(), parentQSpec.offset()))
            .limit(Limits.mergeMin(childQSpec.limit(), parentQSpec.limit()))
            .groupBy(pushGroupBy(childQSpec.groupBy(), parentQSpec.groupBy()))
            .having(pushHaving(childQSpec.having(), parentQSpec.having()))
            .hasAggregates(childQSpec.hasAggregates() || parentQSpec.hasAggregates());
    }


    private static WhereClause mergeWhere(WhereClause where1, WhereClause where2) {
        if (!where1.hasQuery() || where1 == WhereClause.MATCH_ALL) {
            return where2;
        } else if (!where2.hasQuery() || where2 == WhereClause.MATCH_ALL) {
            return where1;
        }

        return new WhereClause(AndOperator.join(ImmutableList.of(where2.query(), where1.query())));
    }

    /**
     * "Merge" OrderBy of child & parent relations.
     * <p/>
     * examples:
     * <pre>
     *      childOrderBy: col1, col2
     *      parentOrderBy: col2, col3, col4
     *
     *      merged OrderBy returned: col2, col3, col4
     * </pre>
     * <p/>
     * <pre>
     *      childOrderBy: col1, col2
     *      parentOrderBy:
     *
     *      merged OrderBy returned: col1, col2
     * </pre>
     *
     * @param childOrderBy  The OrderBy of the relation being processed
     * @param parentOrderBy The OrderBy of the parent relation (outer select,  union, etc.)
     * @return The merged orderBy
     */
    @Nullable
    private static OrderBy tryReplace(Optional<OrderBy> childOrderBy, Optional<OrderBy> parentOrderBy) {
        if (parentOrderBy.isPresent()) {
            return parentOrderBy.get();
        }
        return childOrderBy.orElse(null);
    }


    @Nullable
    private static List<Symbol> pushGroupBy(Optional<List<Symbol>> childGroupBy, Optional<List<Symbol>> parentGroupBy) {
        assert !(childGroupBy.isPresent() && parentGroupBy.isPresent()) :
            "Cannot merge 'group by' if exists in both parent and child relations";
        return childGroupBy.map(Optional::of).orElse(parentGroupBy).orElse(null);
    }

    @Nullable
    private static HavingClause pushHaving(Optional<HavingClause> childHaving, Optional<HavingClause> parentHaving) {
        assert !(childHaving.isPresent() && parentHaving.isPresent()) :
            "Cannot merge 'having' if exists in both parent and child relations";
        return childHaving.map(Optional::of).orElse(parentHaving).orElse(null);
    }

    private static boolean canBeMerged(QuerySpec childQuerySpec, QuerySpec parentQuerySpec) {
        WhereClause parentWhere = parentQuerySpec.where();
        boolean parentHasWhere = !parentWhere.equals(WhereClause.MATCH_ALL);
        boolean childHasLimit = childQuerySpec.limit().isPresent();
        if (parentHasWhere && childHasLimit) {
            return false;
        }

        boolean parentHasAggregations = parentQuerySpec.hasAggregates() || parentQuerySpec.groupBy().isPresent();
        boolean childHasAggregations = childQuerySpec.hasAggregates() || childQuerySpec.groupBy().isPresent();
        if (parentHasAggregations && (childHasLimit || childHasAggregations)) {
            return false;
        }

        Optional<OrderBy> childOrderBy = childQuerySpec.orderBy();
        Optional<OrderBy> parentOrderBy = parentQuerySpec.orderBy();
        if (childHasLimit && childOrderBy.isPresent() && parentOrderBy.isPresent() &&
            !childOrderBy.equals(parentOrderBy)) {
            return false;
        }
        if (parentHasWhere && parentWhere.hasQuery() && Aggregations.containsAggregation(parentWhere.query())) {
            return false;
        }
        return true;
    }

    private final static class FieldReplacer extends ReplacingSymbolVisitor<Void> implements Function<Symbol, Symbol> {

        private final List<? extends Symbol> outputs;
        final HashMap<Symbol, Field> replacedFieldsByNewOutput = new HashMap<>();

        FieldReplacer(List<? extends Symbol> outputs) {
            super(ReplaceMode.COPY);
            this.outputs = outputs;
        }

        @Override
        public Symbol visitField(Field field, Void context) {
            Symbol newOutput = outputs.get(field.index());
            replacedFieldsByNewOutput.put(newOutput, field);
            return newOutput;
        }

        @Override
        public Symbol apply(Symbol symbol) {
            return process(symbol, null);
        }
    }
}
