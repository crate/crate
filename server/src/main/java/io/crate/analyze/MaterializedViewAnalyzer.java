/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package io.crate.analyze;

import java.util.List;
import java.util.UUID;

import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.MaterializedViewMetadata;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.CreateMaterializedView;
import io.crate.sql.tree.Query;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.RefreshMaterializedView;

final class MaterializedViewAnalyzer {

    private final CreateTableAsAnalyzer createTableAsAnalyzer;
    private final Schemas schemas;

    MaterializedViewAnalyzer(CreateTableAsAnalyzer createTableAsAnalyzer, Schemas schemas) {
        this.createTableAsAnalyzer = createTableAsAnalyzer;
        this.schemas = schemas;
    }

    AnalyzedRefreshMaterializedView analyze(RefreshMaterializedView refresh,
                                            CoordinatorTxnCtx txnCtx) {
        DocTableInfo target = schemas.findRelation(
            refresh.name(),
            Operation.READ,
            txnCtx.sessionSettings().sessionUser(),
            txnCtx.sessionSettings().searchPath()
        );
        if (!MaterializedViewMetadata.isMaterialized(target.parameters())) {
            throw new IllegalArgumentException(
                "Relation '" + target.ident().fqn() + "' is not a materialized view");
        }
        Query query = (Query) SqlParser.createStatement(
            MaterializedViewMetadata.definition(target.parameters())
        );
        CoordinatorSessionSettings sessionSettings = txnCtx.sessionSettings();
        CoordinatorSessionSettings refreshSettings = new CoordinatorSessionSettings(
            sessionSettings.authenticatedUser(),
            sessionSettings.sessionUser(),
            MaterializedViewMetadata.searchPath(target.parameters(), sessionSettings.searchPath()),
            sessionSettings.hashJoinsEnabled(),
            sessionSettings.excludedOptimizerRules(),
            sessionSettings.errorOnUnknownObjectKey(),
            sessionSettings.memoryLimitInBytes(),
            sessionSettings.insertSelectFailFast()
        );
        CoordinatorTxnCtx refreshTxnCtx = new CoordinatorTxnCtx(refreshSettings);
        String replacementName = "__crate_mview_" + UUID.randomUUID().toString().replace("-", "");
        QualifiedName replacement = new QualifiedName(List.of(target.ident().schema(), replacementName));
        AnalyzedCreateTableAs createReplacement = createTableAsAnalyzer.analyze(
            new CreateMaterializedView(replacement, query, false),
            ParamTypeHints.EMPTY,
            refreshTxnCtx,
            MaterializedViewMetadata.owner(target.parameters(), sessionSettings.sessionUser().name())
        );
        return new AnalyzedRefreshMaterializedView(target, createReplacement);
    }
}
