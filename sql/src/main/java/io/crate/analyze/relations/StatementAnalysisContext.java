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

package io.crate.analyze.relations;

import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.ParameterContext;
import io.crate.metadata.table.Operation;

import java.util.ArrayList;
import java.util.List;

public class StatementAnalysisContext {

    private final Operation currentOperation;
    private ParameterContext parameterContext;
    private AnalysisMetaData analysisMetaData;
    private List<RelationAnalysisContext> lastRelationContextQueue = new ArrayList<>();
    private RelationAnalysisContext currentRelationContext;

    StatementAnalysisContext(ParameterContext parameterContext, AnalysisMetaData analysisMetaData) {
        this(parameterContext, analysisMetaData, Operation.READ);
    }

    public StatementAnalysisContext(ParameterContext parameterContext,
                                    AnalysisMetaData analysisMetaData,
                                    Operation currentOperation) {
        this.parameterContext = parameterContext;
        this.analysisMetaData = analysisMetaData;
        this.currentOperation = currentOperation;
    }

    public ParameterContext parameterContext() {
        return parameterContext;
    }

    public AnalysisMetaData analysisMetaData() {
        return analysisMetaData;
    }

    Operation currentOperation() {
        return currentOperation;
    }

    public RelationAnalysisContext startRelation() {
        return startRelation(false);
    }

    RelationAnalysisContext startRelation(boolean aliasedRelation) {
        if (currentRelationContext != null) {
            lastRelationContextQueue.add(currentRelationContext);
        }
        currentRelationContext = new RelationAnalysisContext(this, aliasedRelation);
        return currentRelationContext;
    }

    public void endRelation() {
        if (lastRelationContextQueue.size() > 0) {
            currentRelationContext = lastRelationContextQueue.remove(lastRelationContextQueue.size() - 1);
        } else {
            currentRelationContext = null;
        }
    }

    RelationAnalysisContext currentRelationContext() {
        assert currentRelationContext != null : "relation context must be created using startRelation() first";
        return currentRelationContext;
    }
}
