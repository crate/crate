/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.analyze;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.Reference;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class UpdateAnalyzedStatement implements AnalyzedStatement {

    private final List<NestedAnalyzedStatement> nestedStatements;
    private final AnalyzedRelation sourceRelation;

    public UpdateAnalyzedStatement(AnalyzedRelation sourceRelation, List<NestedAnalyzedStatement> nestedStatements) {
        this.sourceRelation = sourceRelation;
        this.nestedStatements = nestedStatements;
    }

    public AnalyzedRelation sourceRelation() {
        return sourceRelation;
    }

    public List<NestedAnalyzedStatement> nestedStatements() {
        return nestedStatements;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitUpdateStatement(this, context);
    }

    public static class NestedAnalyzedStatement {

        private final WhereClause whereClause;
        private final Map<Reference, Symbol> assignments = new HashMap<>();

        public NestedAnalyzedStatement(WhereClause whereClause) {
            this.whereClause = whereClause;
        }

        public Map<Reference, Symbol> assignments() {
            return assignments;
        }

        public WhereClause whereClause() {
            return whereClause;
        }

        public void addAssignment(Reference reference, Symbol value) {
            if (assignments.containsKey(reference)) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "reference repeated %s", reference.ident().columnIdent().sqlFqn()));
            }
            assignments.put(reference, value);
        }
    }

    @Override
    public boolean isWriteOperation() {
        return true;
    }
}
