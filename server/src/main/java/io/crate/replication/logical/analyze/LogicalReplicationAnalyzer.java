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

import java.util.Locale;

import org.elasticsearch.index.IndexSettings;

import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.common.collections.Lists;
import io.crate.exceptions.InvalidArgumentException;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.UnauthorizedException;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.settings.SessionSettings;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.replication.logical.exceptions.PublicationAlreadyExistsException;
import io.crate.replication.logical.exceptions.PublicationUnknownException;
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

    public AnalyzedCreatePublication analyze(CreatePublication createPublication, SessionSettings sessionSettings) {
        if (logicalReplicationService.publications().containsKey(createPublication.name())) {
            throw new PublicationAlreadyExistsException(createPublication.name());
        }

        var defaultSchema = sessionSettings.searchPath().currentSchema();
        var tables = Lists.map(
            createPublication.tables(),
            q -> {
                var relation = RelationName.of(q, defaultSchema);
                if (schemas.getTableInfo(relation) instanceof DocTableInfo tableInfo) {
                    boolean softDeletes;
                    if ((softDeletes = IndexSettings.INDEX_SOFT_DELETES_SETTING.get(tableInfo.parameters())) == false) {
                        throw new UnsupportedOperationException(
                            String.format(
                                Locale.ENGLISH,
                                "Tables included in a publication must have the table setting " +
                                "'soft_deletes.enabled' set to `true`, current setting for table '%s': %b",
                                relation,
                                softDeletes)
                        );
                    }
                } else {
                    throw new RelationUnknown(relation);
                }

                return relation;
            }
        );

        return new AnalyzedCreatePublication(createPublication.name(), createPublication.isForAllTables(), tables);
    }

    public AnalyzedDropPublication analyze(DropPublication dropPublication,
                                           CoordinatorSessionSettings sessionSettings) {
        var publication = logicalReplicationService.publications().get(dropPublication.name());
        if (dropPublication.ifExists() == false && publication == null) {
            throw new PublicationUnknownException(dropPublication.name());
        }
        if (publication != null
            && publication.owner().equals(sessionSettings.sessionUser().name()) == false
            && sessionSettings.sessionUser().isSuperUser() == false) {
            throw new UnauthorizedException("A publication can only be dropped by the owner or a superuser");
        }

        return new AnalyzedDropPublication(dropPublication.name(), dropPublication.ifExists());
    }

    public AnalyzedAlterPublication analyze(AlterPublication alterPublication,
                                            CoordinatorSessionSettings sessionSettings) {
        var publication = logicalReplicationService.publications().get(alterPublication.name());
        if (publication == null) {
            throw new PublicationUnknownException(alterPublication.name());
        }
        if (publication.owner().equals(sessionSettings.sessionUser().name()) == false
            && sessionSettings.sessionUser().isSuperUser() == false) {
            throw new UnauthorizedException("A publication can only be altered by the owner or a superuser");
        }
        if (publication.isForAllTables()) {
            throw new InvalidArgumentException(
                "Publication '" + alterPublication.name() + "' is defined as FOR ALL TABLES," +
                " adding or dropping tables is not supported"
            );
        }
        var defaultSchema = sessionSettings.searchPath().currentSchema();
        var tables = Lists.map(
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
            txnCtx, nodeCtx, paramTypeHints, FieldProvider.TO_LITERAL_VALIDATE_NAME, null);
        var exprCtx = new ExpressionAnalysisContext(txnCtx.sessionSettings());
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

    public AnalyzedDropSubscription analyze(DropSubscription dropSubscription, CoordinatorSessionSettings sessionSettings) {
        var subscription = logicalReplicationService.subscriptions().get(dropSubscription.name());
        if (dropSubscription.ifExists() == false && subscription == null) {
            throw new SubscriptionUnknownException(dropSubscription.name());
        }
        if (subscription != null
            && subscription.owner().equals(sessionSettings.sessionUser().name()) == false
            && sessionSettings.sessionUser().isSuperUser() == false) {
            throw new UnauthorizedException("A subscription can only be dropped by the owner or a superuser");
        }
        return new AnalyzedDropSubscription(dropSubscription.name(), subscription, dropSubscription.ifExists());
    }

    public AnalyzedAlterSubscription analyze(AlterSubscription alterSubscription, CoordinatorSessionSettings sessionSettings) {
        var subscription = logicalReplicationService.subscriptions().get(alterSubscription.name());
        if (subscription == null) {
            throw new SubscriptionUnknownException(alterSubscription.name());
        }
        if (subscription.owner().equals(sessionSettings.sessionUser().name()) == false
            && sessionSettings.sessionUser().isSuperUser() == false) {
            throw new UnauthorizedException("A subscription can only be altered by the owner or a superuser");
        }

        return new AnalyzedAlterSubscription(
            alterSubscription.name(),
            alterSubscription.mode() == AlterSubscription.Mode.ENABLE
        );
    }
}
