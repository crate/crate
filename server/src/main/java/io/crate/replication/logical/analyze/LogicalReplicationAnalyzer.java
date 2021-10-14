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

package io.crate.replication.logical.analyze;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.common.collections.Lists2;
import io.crate.replication.logical.exceptions.PublicationAlreadyExistsException;
import io.crate.replication.logical.exceptions.PublicationUnknownException;
import io.crate.exceptions.RelationUnknown;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.replication.logical.exceptions.SubscriptionAlreadyExistsException;
import io.crate.replication.logical.exceptions.SubscriptionUnknownException;
import io.crate.sql.tree.AlterPublication;
import io.crate.sql.tree.AlterSubscription;
import io.crate.sql.tree.CreatePublication;
import io.crate.sql.tree.CreateSubscription;
import io.crate.sql.tree.DropPublication;
import io.crate.sql.tree.DropSubscription;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;

public class LogicalReplicationAnalyzer {

    private final Schemas schemas;
    private final LogicalReplicationService logicalReplicationService;
    private final NodeContext nodeCtx;

    public LogicalReplicationAnalyzer(Schemas schemas,
                                      LogicalReplicationService logicalReplicationService,
                                      NodeContext nodeCtx) {
        this.schemas = schemas;
        this.logicalReplicationService = logicalReplicationService;
        this.nodeCtx = nodeCtx;
    }

    public AnalyzedCreatePublication analyze(CreatePublication createPublication,
                                             SessionContext sessionContext) {
        if (logicalReplicationService.publications().containsKey(createPublication.name())) {
            throw new PublicationAlreadyExistsException(createPublication.name());
        }

        var defaultSchema = sessionContext.searchPath().currentSchema();
        var tables = Lists2.map(
            createPublication.tables(),
            q -> {
                var relation = RelationName.of(q, defaultSchema);
                if (schemas.tableExists(relation) == false) {
                    throw new RelationUnknown(relation);
                }
                return relation;
            }
        );

        return new AnalyzedCreatePublication(createPublication.name(), createPublication.isForAllTables(), tables);
    }

    public AnalyzedDropPublication analyze(DropPublication dropPublication) {
        if (dropPublication.ifExists() == false
            && logicalReplicationService.publications().containsKey(dropPublication.name()) == false) {
            throw new PublicationUnknownException(dropPublication.name());
        }

        return new AnalyzedDropPublication(dropPublication.name(), dropPublication.ifExists());
    }

    public AnalyzedAlterPublication analyze(AlterPublication alterPublication,
                                            SessionContext sessionContext) {
        if (logicalReplicationService.publications().containsKey(alterPublication.name()) == false) {
            throw new PublicationUnknownException(alterPublication.name());
        }
        var defaultSchema = sessionContext.searchPath().currentSchema();
        var tables = Lists2.map(
            alterPublication.tables(),
            q -> {
                var relation = RelationName.of(q, defaultSchema);
                if (schemas.tableExists(relation) == false) {
                    throw new RelationUnknown(relation);
                }
                return relation;
            }
        );
        return new AnalyzedAlterPublication(alterPublication.name(), alterPublication.operation(), tables);
    }

    public AnalyzedCreateSubscription analyze(CreateSubscription<Expression> createSubscription,
                                              ParamTypeHints paramTypeHints,
                                              CoordinatorTxnCtx txnCtx) {
        if (logicalReplicationService.subscriptions().containsKey(createSubscription.name())) {
            throw new SubscriptionAlreadyExistsException(createSubscription.name());
        }

        var expressionAnalyzer = new ExpressionAnalyzer(
            txnCtx, nodeCtx, paramTypeHints, FieldProvider.FIELDS_AS_LITERAL, null);
        var exprCtx = new ExpressionAnalysisContext(txnCtx.sessionContext());
        GenericProperties<Symbol> genericProperties = createSubscription.properties()
            .map(p -> expressionAnalyzer.convert(p, exprCtx));
        Symbol connectionInfo = expressionAnalyzer.convert(createSubscription.connectionInfo(), exprCtx);

        return new AnalyzedCreateSubscription(
            createSubscription.name(),
            connectionInfo,
            createSubscription.publications(),
            genericProperties
        );
    }

    public AnalyzedDropSubscription analyze(DropSubscription dropSubscription) {
        if (dropSubscription.ifExists() == false
            && logicalReplicationService.subscriptions().containsKey(dropSubscription.name()) == false) {
            throw new SubscriptionUnknownException(dropSubscription.name());
        }
        return new AnalyzedDropSubscription(dropSubscription.name(), dropSubscription.ifExists());
    }

    public AnalyzedAlterSubscription analyze(AlterSubscription alterSubscription) {
        if (logicalReplicationService.subscriptions().containsKey(alterSubscription.name()) == false) {
            throw new SubscriptionUnknownException(alterSubscription.name());
        }

        return new AnalyzedAlterSubscription(
            alterSubscription.name(),
            alterSubscription.mode() == AlterSubscription.Mode.ENABLE
        );
    }
}
