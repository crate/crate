/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.analyze.relations;

import java.util.LinkedHashSet;
import java.util.Set;

import io.crate.analyze.JoinRelation;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.metadata.RelationName;

public class RelationNameExtractor {

    private static Visitor VISITOR = new Visitor();

    public static Set<RelationName> collect(AnalyzedRelation analyzedRelation) {
        var context = new LinkedHashSet<RelationName>();
        analyzedRelation.accept(VISITOR, context);
        return context;
    }

    private static class Visitor extends AnalyzedRelationVisitor<Set<RelationName>, Void> {

        @Override
        protected Void visitAnalyzedRelation(AnalyzedRelation relation, Set<RelationName> context) {
            context.add(relation.relationName());
            return null;
        }

        @Override
        public Void visitUnionSelect(UnionSelect unionSelect, Set<RelationName> context) {
            unionSelect.left().accept(this, context);
            unionSelect.right().accept(this, context);
            return null;
        }

        @Override
        public Void visitTableRelation(TableRelation tableRelation, Set<RelationName> context) {
            context.add(tableRelation.relationName());
            return null;
        }

        @Override
        public Void visitDocTableRelation(DocTableRelation relation, Set<RelationName> context) {
           context.add(relation.relationName());
           return null;
        }

        @Override
        public Void visitTableFunctionRelation(TableFunctionRelation tableFunctionRelation, Set<RelationName> context) {
            context.add(tableFunctionRelation.relationName());
            return null;
        }

        @Override
        public Void visitQueriedSelectRelation(QueriedSelectRelation relation, Set<RelationName> context) {
            for (AnalyzedRelation analyzedRelation : relation.from()) {
                analyzedRelation.accept(this, context);
            }
            return null;
        }

        @Override
        public Void visitView(AnalyzedView analyzedView, Set<RelationName> context) {
            context.add(analyzedView.relationName());
            return null;
        }

        @Override
        public Void visitAliasedAnalyzedRelation(AliasedAnalyzedRelation relation, Set<RelationName> context) {
            context.add(relation.relationName());
            return null;
        }


        @Override
        public Void visitJoinRelation(JoinRelation joinRelation, Set<RelationName> context) {
            joinRelation.left().accept(this, context);
            joinRelation.right().accept(this, context);
            return null;
        }
    }
}
