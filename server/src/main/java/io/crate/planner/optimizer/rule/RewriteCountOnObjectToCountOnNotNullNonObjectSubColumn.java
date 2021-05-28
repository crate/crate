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

package io.crate.planner.optimizer.rule;

import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.execution.engine.aggregation.impl.CountAggregation;
import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.Eval;
import io.crate.planner.operators.GroupHashAggregate;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.planner.optimizer.matcher.PatternWithCaptureValidation;
import io.crate.statistics.TableStats;
import io.crate.types.ObjectType;
import org.elasticsearch.Version;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;

public class RewriteCountOnObjectToCountOnNotNullNonObjectSubColumn implements Rule<GroupHashAggregate> {

    private final Pattern<GroupHashAggregate> pattern;
    private final Capture<Set> notNullSubcolumns;
    private final Capture<Map> countedObjects;

    public RewriteCountOnObjectToCountOnNotNullNonObjectSubColumn() {
        this.countedObjects = new Capture<>();
        this.notNullSubcolumns = new Capture<>();
        var pattern = typeOf(GroupHashAggregate.class)
            .with(plan -> Optional.ofNullable(getNotNullColumnsFromBaseTables(plan.baseTables())),
                  typeOf(Set.class).capturedAs(notNullSubcolumns)
            )
            .with(plan -> Optional.ofNullable(new ObjectTypeArgToFunctionMapper().getObjectTypeArgToFunctionMap(plan.aggregates())),
                  typeOf(Map.class).capturedAs(countedObjects));

        Predicate<Captures> capturesValidation = (captures) -> {
            Map<String, ColumnIdent> notNullColumns = columnIdentToMap(captures.get(notNullSubcolumns));
            Map<ColumnIdent, Function> objectCandidates = captures.get(countedObjects);
            for (ColumnIdent candidateObject : objectCandidates.keySet()) {
                /*
                 * currently not handling the case where the object in count(object) expression is aliased.
                 * need to find the Collect logicalPlan that contains the aliased object as part of its output.
                 * Then it will be the one that needs to be rewritten to collect its subcolumn.
                 */
                if (notNullColumns.containsKey(candidateObject.name())) {
                    return true;
                }
            }
            return false;
        };
        this.pattern = new PatternWithCaptureValidation<>(pattern, capturesValidation);
    }

    @Override
    public Pattern<GroupHashAggregate> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(GroupHashAggregate plan,
                             Captures captures,
                             TableStats tableStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx) {
        Map<String, ColumnIdent> notNullColumns = columnIdentToMap(captures.get(notNullSubcolumns));
        Map<ColumnIdent, Function> objectCandidates = captures.get(countedObjects);

        return new PlanRewriter(notNullColumns, objectCandidates).rewrite(plan);
    }

    private Map<String, ColumnIdent> columnIdentToMap(Set<ColumnIdent> cols) {
        Map<String, ColumnIdent> map = new HashMap<>();
        for (var col : cols) {
            map.put(col.name(), col);
        }
        return map;
    }

    @Override
    public Version requiredVersion() {
        return Version.V_4_1_0;
    }

    public class PlanRewriter {

        Map<String, ColumnIdent> notNullColumns;
        Map<ColumnIdent, Function> objectCandidates;
        Map<String, Reference> rewrittenObjects;
        Map<String, AliasSymbol> rewrittenFunctions;

        public PlanRewriter(Map<String, ColumnIdent> notNullColumns,
                            Map<ColumnIdent, Function> objectCandidates) {
            this.notNullColumns = notNullColumns;
            this.objectCandidates = objectCandidates;
            this.rewrittenFunctions = new HashMap<>();
            this.rewrittenObjects = new HashMap<>();
        }

        LogicalPlan rewrite(GroupHashAggregate orig) {
            rewriteFunctions();
            var newPlan = rewriteGroupHashAggregate(orig);
            var temp = new ArrayList<Symbol>();
            for (var o : orig.outputs()) {
                if (o instanceof Function f) {
                    temp.add(new AliasSymbol(f.toString(), visitFunction(f)));
                } else {
                    temp.add(o);
                }
            }
            return Eval.createUnprunable(newPlan, temp);
        }

        GroupHashAggregate rewriteGroupHashAggregate(GroupHashAggregate orig) {
            var newFunctions = rewrittenFunctions.values().stream().map(AliasSymbol::symbol).map(x -> (Function) x).toList();
            return new GroupHashAggregate(rewriteCollect((Collect) orig.source()),
                                          orig.groupKeys(),
                                          newFunctions,
                                          orig.numExpectedRows());
        }

        Collect rewriteCollect(Collect orig) {
            var newOutputs = orig.outputs().stream().map(o -> {
                if (o instanceof Reference ref) {
                    if (rewrittenObjects.containsKey(ref.column().name())) {
                        return rewrittenObjects.get(ref.column().name());
                    }
                }
                return o;
            }).toList();
            return new Collect(orig.preferSourceLookup(),
                               orig.relation(),
                               newOutputs,
                               orig.where(),
                               orig.numExpectedRows(),
                               orig.estimatedRowSize());
        }

        Map<String, AliasSymbol> rewriteFunctions() {
            for (var e : objectCandidates.entrySet()) {
                var obj = e.getKey();
                var f = e.getValue();
                if (objectCandidates.containsKey(obj)) {
                    String alias = f.toString();
                    rewrittenFunctions.putIfAbsent(obj.name(), new AliasSymbol(alias, visitFunction(f)));
                }
            }
            return rewrittenFunctions;
        }

        public Function visitFunction(Function f) {
            if (CountAggregation.NAME.equals(f.name())) {
                var newArgs = f.arguments().stream()
                    .map(arg -> {
                        if (arg instanceof Reference ref) {
                            rewrittenObjects.putIfAbsent(ref.column().name(), visitReference(ref));
                            return rewrittenObjects.get(ref.column().name());
                        }
                        return arg;
                    }).toList();
                return new Function(f.signature(),
                                    newArgs,
                                    f.valueType(),
                                    f.filter());
            }
            return f;
        }

        public Reference visitReference(Reference symbol) {
            if (symbol.valueType().id() == ObjectType.ID) {
                if (notNullColumns.containsKey(symbol.column().name())) {
                    return new Reference(
                        new ReferenceIdent(symbol.ident().tableIdent(),
                                           symbol.column().append(notNullColumns.get(symbol.column().name()).path().get(
                                               0))),
                        symbol.granularity(),
                        ((ObjectType) symbol.valueType()).innerType(notNullColumns.get(symbol.column().name()).path().get(
                            0)),
                        symbol.columnPolicy(),
                        Reference.IndexType.NOT_ANALYZED,
                        symbol.isNullable(),
                        symbol.isColumnStoreDisabled(),
                        3,
                        symbol.defaultExpression()
                    );
                }
            }
            return symbol;
        }
    }

    public static class ObjectTypeArgToFunctionMapper extends SymbolVisitor<Void, ColumnIdent> {

        /*
         * ex) count(obj1), count(obj2) becomes
         *          obj1 -> count(obj1)
         *          obj2 -> count(obj2)
         */

        private Map<ColumnIdent, Function> map;

        public ObjectTypeArgToFunctionMapper() {
            this.map = new HashMap<>();
        }

        public Map<ColumnIdent, Function> getObjectTypeArgToFunctionMap(List<Function> fs) {
            for (var f : fs) {
                if (f.arguments().size() != 1) {
                    return null;
                }
                var columnIdent = visitFunction(f, null);
                if (columnIdent != null) {
                    map.putIfAbsent(columnIdent, f);
                }
            }
            return map;
        }

        @Override
        public ColumnIdent visitFunction(Function f, Void context) {
            var arg = f.arguments().get(0);
            if (CountAggregation.NAME.equals(f.signature().getName().name())) {
                if (arg.valueType().id() == ObjectType.ID) {
                    return arg.accept(this, null);
                }
            }
            return null;
        }

        @Override
        public ColumnIdent visitReference(Reference symbol, Void context) {
            return symbol.column();
        }

        @Override
        public ColumnIdent visitField(ScopedSymbol field, Void context) {
            return field.column();
        }

        @Override
        public ColumnIdent visitAlias(AliasSymbol aliasSymbol, Void context) {
            return aliasSymbol.symbol().accept(this, null);
        }
    }

    public Set<ColumnIdent> getNotNullColumnsFromBaseTables(List<AbstractTableRelation<?>> baseTables) {
        return baseTables.stream()
            .distinct()
            .filter(t -> t instanceof DocTableRelation)
            .map(t -> (DocTableRelation) t)
            .map(DocTableRelation::tableInfo)
            .map(DocTableInfo::notNullColumns)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());
    }
}
