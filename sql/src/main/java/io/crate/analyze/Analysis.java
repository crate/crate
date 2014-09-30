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

import com.google.common.collect.ImmutableList;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;

import java.util.List;

public abstract class Analysis {

    private final Analyzer.ParameterContext parameterContext;
    private List<String> outputNames = ImmutableList.of();

    protected Analysis(Analyzer.ParameterContext parameterContext) {
        this.parameterContext = parameterContext;
    }

    @Deprecated
    public abstract void table(TableIdent tableIdent);

    @Deprecated
    public abstract TableInfo table();

    public abstract boolean hasNoResult();

    public abstract void normalize();

    @Deprecated
    public void outputNames(List<String> outputNames) {
        this.outputNames = outputNames;
    }

    @Deprecated
    public List<String> outputNames() {
        return outputNames;
    }

    public Analyzer.ParameterContext parameterContext() {
        return parameterContext;
    }

    public Object[] parameters() {
        return parameterContext.parameters();
    }

    public <C, R> R accept(AnalysisVisitor<C,R> analysisVisitor, C context) {
        return analysisVisitor.visitAnalysis(this, context);
    }

    public boolean isData() {
        return true;
    }

}
