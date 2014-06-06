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

package io.crate.analyze;

import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.ReferenceResolver;
import io.crate.planner.symbol.Symbol;

import java.util.List;

public class InsertFromSubQueryAnalysis extends AbstractInsertAnalysis {

    private final SelectAnalysis subQueryAnalysis;

    public InsertFromSubQueryAnalysis(ReferenceInfos referenceInfos,
                                      Functions functions,
                                      Object[] parameters,
                                      ReferenceResolver referenceResolver) {
        super(referenceInfos, functions, parameters, referenceResolver);
        this.subQueryAnalysis = new SelectAnalysis(referenceInfos, functions, parameters, referenceResolver);
    }

    public SelectAnalysis subQueryAnalysis() {
        return this.subQueryAnalysis;
    }

    public List<Symbol> getSubQueryColumns() {
       return  this.subQueryAnalysis.outputSymbols();
    }

    @Override
    public <C, R> R accept(AnalysisVisitor<C, R> analysisVisitor, C context) {
        return analysisVisitor.visitInsertFromSubQueryAnalysis(this, context);
    }
}
