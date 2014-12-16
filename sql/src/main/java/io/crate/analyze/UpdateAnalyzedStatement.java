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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.ReferenceResolver;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;

import javax.annotation.Nullable;
import java.util.*;

public class UpdateAnalyzedStatement extends AbstractDataAnalyzedStatement {

    private static final Predicate<NestedAnalyzedStatement> HAS_NO_RESULT_PREDICATE = new Predicate<NestedAnalyzedStatement>() {
        @Override
        public boolean apply(@Nullable NestedAnalyzedStatement input) {
            return input != null && input.hasNoResult();
        }
    };

    List<NestedAnalyzedStatement> nestedAnalysisList;


    public UpdateAnalyzedStatement(ReferenceInfos referenceInfos,
                                   Functions functions,
                                   ParameterContext parameterContext,
                                   ReferenceResolver referenceResolver) {
        super(referenceInfos, functions, parameterContext, referenceResolver);
        int numNested = 1;
        if (parameterContext.bulkParameters.length > 0) {
            numNested = parameterContext.bulkParameters.length;
        }

        nestedAnalysisList = new ArrayList<>(numNested);
        for (int i = 0; i < numNested; i++) {
            nestedAnalysisList.add(new NestedAnalyzedStatement(
                    referenceInfos,
                    functions,
                    parameterContext,
                    referenceResolver
            ));
        }
    }

    @Override
    public boolean expectsAffectedRows() {
        return true;
    }

    @Override
    public void table(TableIdent tableIdent) {
        throw new UnsupportedOperationException("used nested analysis");
    }

    @Override
    public TableInfo table() {
        throw new UnsupportedOperationException("used nested analysis");
    }

    @Override
    public boolean hasNoResult() {
        return Iterables.all(nestedAnalysisList, HAS_NO_RESULT_PREDICATE);
    }

    @Override
    public void normalize() {

    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitUpdateStatement(this, context);
    }

    public List<NestedAnalyzedStatement> nestedAnalysis() {
        return nestedAnalysisList;
    }

    public static class NestedAnalyzedStatement extends AbstractDataAnalyzedStatement {

        private Map<Reference, Symbol> assignments = new HashMap<>();

        public NestedAnalyzedStatement(ReferenceInfos referenceInfos,
                                       Functions functions,
                                       ParameterContext parameterContext,
                                       ReferenceResolver referenceResolver) {
            super(referenceInfos, functions, parameterContext, referenceResolver);
        }

        @Override
        public boolean hasNoResult() {
            return whereClause().noMatch();
        }

        public Map<Reference, Symbol> assignments() {
            return assignments;
        }

        public void addAssignment(Reference reference, Symbol value) {
            if (assignments.containsKey(reference)) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "reference repeated %s", reference.info().ident().columnIdent().sqlFqn()));
            }
            if (!reference.info().ident().tableIdent().equals(table().ident())) {
                throw new UnsupportedOperationException("cannot update references from other tables.");
            }
            assignments.put(reference, value);
        }
    }
}
