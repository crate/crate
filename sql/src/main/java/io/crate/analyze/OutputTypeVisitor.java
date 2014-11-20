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

import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;

/**
 * extract DataTypes of output columns from an analysis
 *
 * This visitor can be used to extract datatypes
 */
public class OutputTypeVisitor extends AnalysisVisitor<Void, DataType[]> {

    public static final DataType[] EMPTY_TYPES = new DataType[0];

    public DataType[] process(AnalyzedStatement analyzedStatement) {
        return super.process(analyzedStatement, null);
    }

    @Override
    protected DataType[] visitAnalysis(AnalyzedStatement analyzedStatement, Void context) {
        return EMPTY_TYPES;
    }

    @Override
    protected DataType[] visitSelectAnalysis(SelectAnalyzedStatement analysis, Void context) {
        DataType[] types = new DataType[analysis.outputSymbols().size()];
        java.util.List<Symbol> outputSymbols = analysis.outputSymbols();
        for (int i = 0, outputSymbolsSize = outputSymbols.size(); i < outputSymbolsSize; i++) {
            types[i] = outputSymbols.get(i).valueType();
        }
        return types;
    }
}
