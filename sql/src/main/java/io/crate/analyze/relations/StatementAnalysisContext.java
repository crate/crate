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

import io.crate.action.sql.SessionContext;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.ParameterExpression;
import io.crate.sql.tree.QualifiedName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class StatementAnalysisContext {

    private final Operation currentOperation;
    private final TransactionContext transactionContext;
    private final SessionContext sessionContext;
    private final Function<ParameterExpression, Symbol> convertParamFunction;
    private final List<RelationAnalysisContext> lastRelationContextQueue = new ArrayList<>();

    public StatementAnalysisContext(SessionContext sessionContext,
                                    Function<ParameterExpression, Symbol> convertParamFunction,
                                    Operation currentOperation,
                                    TransactionContext transactionContext) {
        this.sessionContext = sessionContext;
        this.convertParamFunction = convertParamFunction;
        this.currentOperation = currentOperation;
        this.transactionContext = transactionContext;
    }

    public TransactionContext transactionContext() {
        return transactionContext;
    }

    Operation currentOperation() {
        return currentOperation;
    }

    public RelationAnalysisContext startRelation() {
        return startRelation(false);
    }

    RelationAnalysisContext startRelation(boolean aliasedRelation) {
        Map<QualifiedName, AnalyzedRelation> parentSources;
        if (lastRelationContextQueue.isEmpty()) {
            parentSources = Collections.emptyMap();
        } else {
            parentSources = lastRelationContextQueue.get(lastRelationContextQueue.size() - 1).sources();
        }
        RelationAnalysisContext currentRelationContext = new RelationAnalysisContext(aliasedRelation, parentSources);
        lastRelationContextQueue.add(currentRelationContext);
        return currentRelationContext;
    }

    public void endRelation() {
        if (lastRelationContextQueue.size() > 0) {
            lastRelationContextQueue.remove(lastRelationContextQueue.size() - 1);
        }
    }

    RelationAnalysisContext currentRelationContext() {
        assert lastRelationContextQueue.size() > 0 : "relation context must be created using startRelation() first";
        return lastRelationContextQueue.get(lastRelationContextQueue.size() - 1);
    }

    public SessionContext sessionContext() {
        return sessionContext;
    }

    public Function<ParameterExpression,Symbol> convertParamFunction() {
        return convertParamFunction;
    }
}
