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

public class UpdateAnalysis extends Analysis {

    private static final Predicate<NestedAnalysis> HAS_NO_RESULT_PREDICATE = new Predicate<NestedAnalysis>() {
        @Override
        public boolean apply(@Nullable NestedAnalysis input) {
            return input != null && input.hasNoResult();
        }
    };

    List<NestedAnalysis> nestedAnalysisList;


    public UpdateAnalysis(ReferenceInfos referenceInfos,
                          Functions functions,
                          Analyzer.ParameterContext parameterContext,
                          ReferenceResolver referenceResolver) {
        super(parameterContext);
        int numNested = 1;
        if (parameterContext.bulkParameters.length > 0) {
            numNested = parameterContext.bulkParameters.length;
        }

        nestedAnalysisList = new ArrayList<>(numNested);
        for (int i = 0; i < numNested; i++) {
            nestedAnalysisList.add(new NestedAnalysis(
                    referenceInfos,
                    functions,
                    parameterContext,
                    referenceResolver
            ));
        }
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
    public <C, R> R accept(AnalysisVisitor<C, R> analysisVisitor, C context) {
        return analysisVisitor.visitUpdateAnalysis(this, context);
    }

    public List<NestedAnalysis> nestedAnalysis() {
        return nestedAnalysisList;
    }

    public static class NestedAnalysis extends AbstractDataAnalysis {

        private Map<Reference, Symbol> assignments = new HashMap<>();

        public NestedAnalysis(ReferenceInfos referenceInfos,
                              Functions functions,
                              Analyzer.ParameterContext parameterContext,
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
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "reference repeated %s", reference.info().ident().columnIdent().fqn()));
            }
            if (!reference.info().ident().tableIdent().equals(table().ident())) {
                throw new UnsupportedOperationException("cannot update references from other tables.");
            }
            assignments.put(reference, value);
        }
    }
}
