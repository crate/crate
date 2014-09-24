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

import com.google.common.collect.Sets;
import io.crate.analyze.where.WhereClause;
import io.crate.metadata.TableIdent;
import io.crate.operation.operator.Operators;
import io.crate.planner.symbol.*;

import javax.annotation.Nullable;
import java.util.*;

/**
 * Class to generate an AnalyzedRelation (JoinRelation) from a list of relations.
 * <p>
 * Used to convert something like
 * <pre>
 *     select * from a, b
 * </pre>
 * into
 *
 * <pre>
 *     cross_join(a, b)
 * </pre>
 * </p>
 */
public class JoinDetector {

    private final static JoinCriteriaExtractor CRITERIA_EXTRACTOR = new JoinCriteriaExtractor();
    private final static ReferenceExtractor REFERENCE_EXTRACTOR = new ReferenceExtractor();

    /**
     * generate a join relation from a list of relations. See class description for more info.
     *
     * @param relations a list of relations (e.g. {@link io.crate.metadata.table.TableInfo}
     * @param whereClause whereClause of the query: is inspected for join criteria to determine the join type
     * @return an analyzed relation.<br />
     *      If the relations contained more than 1 element this will be a {@link io.crate.metadata.relation.JoinRelation} <br />
     *      In case the relations only had one item that item is returned as is.
     * @throws java.lang.UnsupportedOperationException in case the whereClause contains joinCriteria that aren't supported yet
     */
    public static AnalyzedRelation buildRelation(List<AnalyzedRelation> relations, WhereClause whereClause) {
        if (relations.size() == 1) {
            return relations.get(0);
        }
        if (!whereClause.hasQuery()) {
            LinkedList<AnalyzedRelation> mutableRelations = new LinkedList<>(relations);
            return buildNestedJoin(JoinRelation.Type.CROSS_JOIN, mutableRelations, null);
        }

        JoinCriteriaContext ctx = new JoinCriteriaContext();
        CRITERIA_EXTRACTOR.process(whereClause.query(), ctx);
        if (ctx.joinCriteria.isEmpty()) {
            LinkedList<AnalyzedRelation> mutableRelations = new LinkedList<>(relations);
            return buildNestedJoin(JoinRelation.Type.CROSS_JOIN, mutableRelations, null);
        }
        throw new UnsupportedOperationException(
                "Detected join criteria in the WHERE clause but only cross joins are supported");
    }

    private static AnalyzedRelation buildNestedJoin(JoinRelation.Type type,
                                                    List<AnalyzedRelation> relations,
                                                    @Nullable AnalyzedRelation lastRelation) {
        if (lastRelation != null) {
            if (relations.size() > 0) {
                AnalyzedRelation firstRelation = relations.remove(0);
                return buildNestedJoin(type, relations, new JoinRelation(type, lastRelation, firstRelation));
            }
            return lastRelation;
        }
        if (relations.size() == 2) {
            return new JoinRelation(type, relations.get(0), relations.get(1));
        }

        AnalyzedRelation left = relations.remove(0);
        AnalyzedRelation right = relations.remove(0);
        return buildNestedJoin(type, relations, new JoinRelation(type, left, right));
    }

    public static class ReferenceContext {
        List<Reference> references = new ArrayList<>();
    }

    public static class ReferenceExtractor extends SymbolVisitor<ReferenceContext, Void> {

        public List<Reference> extract(Symbol symbol) {
            ReferenceContext ctx = new ReferenceContext();
            process(symbol, ctx);
            return ctx.references;
        }

        @Override
        public Void visitReference(Reference symbol, ReferenceContext context) {
            context.references.add(symbol);
            return null;
        }

        @Override
        public Void visitDynamicReference(DynamicReference symbol, ReferenceContext context) {
            return visitReference(symbol, context);
        }

        @Override
        public Void visitFunction(Function symbol, ReferenceContext context) {
            for (Symbol arg : symbol.arguments()) {
                process(arg, context);
            }
            return null;
        }
    }

    private static class JoinCriteriaContext {
        List<Function> joinCriteria = new ArrayList<>();

        public void addCriteria(Function function) {
            joinCriteria.add(function);
        }
    }

    private static class JoinCriteriaExtractor extends SymbolVisitor<JoinCriteriaContext, Void> {

        @Override
        public Void visitFunction(Function function, JoinCriteriaContext context) {
            if (Operators.COMPARATORS.contains(function.info().ident().name()) && function.arguments().size() == 2) {
                List<Reference> leftRefs = REFERENCE_EXTRACTOR.extract(function.arguments().get(0));
                List<Reference> rightRefs = REFERENCE_EXTRACTOR.extract(function.arguments().get(1));

                Set<TableIdent> leftTables = extractTableIdentifier(leftRefs);
                Set<TableIdent> rightTables = extractTableIdentifier(rightRefs);

                if (leftTables.size() > 1 || rightTables.size() > 1 || Sets.union(leftTables, rightTables).size() > 1) {
                    context.addCriteria(function);
                }
            } else {
                for (Symbol argument : function.arguments()) {
                    process(argument, context);
                }
            }
            return null;
        }

        private Set<TableIdent> extractTableIdentifier(List<Reference> references) {
            Set<TableIdent> result = new HashSet<>();
            for (Reference ref : references) {
                result.add(ref.info().ident().tableIdent());
            }
            return result;
        }
    }
}
