/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.logging.DeprecationLogger;

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.common.collections.Lists2;
import io.crate.execution.ddl.RepositoryService;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.udf.UserDefinedFunctionsMetadata;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.settings.AnalyzerSettings;
import io.crate.metadata.view.ViewsMetadata;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.RestoreSnapshot;
import io.crate.sql.tree.Table;
import io.crate.user.metadata.RolesMetadata;
import io.crate.user.metadata.UsersMetadata;
import io.crate.user.metadata.UsersPrivilegesMetadata;

class RestoreSnapshotAnalyzer {

    static final DeprecationLogger DEPRECATION_LOGGER =
        new DeprecationLogger(LogManager.getLogger(RestoreSnapshotAnalyzer.class));

    public static final List<String> USER_MANAGEMENT_METADATA = List.of(
        UsersMetadata.TYPE, // Also restore old UsersMetadata
        RolesMetadata.TYPE,
        UsersPrivilegesMetadata.TYPE);

    public static final Map<String, List<String>> METADATA_CUSTOM_TYPE_MAP = Map.of(
        "VIEWS", List.of(ViewsMetadata.TYPE),
        "UDFS", List.of(UserDefinedFunctionsMetadata.TYPE),
        "USERS", USER_MANAGEMENT_METADATA,        // Deprecated, keeping for BWC
        "PRIVILEGES", USER_MANAGEMENT_METADATA,   // Deprecated, keeping for BWC
        "USERMANAGEMENT", USER_MANAGEMENT_METADATA
    );

    private final RepositoryService repositoryService;
    private final NodeContext nodeCtx;

    RestoreSnapshotAnalyzer(RepositoryService repositoryService, NodeContext nodeCtx) {
        this.repositoryService = repositoryService;
        this.nodeCtx = nodeCtx;
    }

    public AnalyzedRestoreSnapshot analyze(RestoreSnapshot<Expression> restoreSnapshot,
                                           ParamTypeHints paramTypeHints,
                                           CoordinatorTxnCtx txnCtx) {
        List<String> nameParts = restoreSnapshot.name().getParts();
        if (nameParts.size() != 2) {
            throw new IllegalArgumentException(
                "Snapshot name not supported, only <repository>.<snapshot> works.)");
        }
        var repositoryName = nameParts.get(0);
        var snapshotName = nameParts.get(1);
        repositoryService.failIfRepositoryDoesNotExist(repositoryName);

        var exprCtx = new ExpressionAnalysisContext(txnCtx.sessionSettings());
        var exprAnalyzerWithoutFields = new ExpressionAnalyzer(
            txnCtx,
            nodeCtx,
            paramTypeHints,
            FieldProvider.UNSUPPORTED,
            null
        );
        GenericProperties<Symbol> properties = restoreSnapshot.properties()
            .map(x -> exprAnalyzerWithoutFields.convert(x, exprCtx));

        List<Table<Symbol>> tables = List.of();
        HashSet<String> customMetadataTypes = new HashSet<>();
        ArrayList<String> globalSettings = new ArrayList<>();
        boolean includeTables = false;
        boolean includeCustomMetadata = false;
        boolean includeGlobalSettings = false;

        switch (restoreSnapshot.mode()) {
            case ALL -> {
                includeTables = true;
                includeCustomMetadata = true;
                includeGlobalSettings = true;
            }
            case METADATA -> {
                includeCustomMetadata = true;
                includeGlobalSettings = true;
                globalSettings.add(AnalyzerSettings.CUSTOM_ANALYSIS_SETTINGS_PREFIX);
            }
            case TABLE -> {
                includeTables = true;
                var exprAnalyzerWithFieldsAsString = new ExpressionAnalyzer(
                    txnCtx,
                    nodeCtx,
                    paramTypeHints,
                    FieldProvider.TO_LITERAL_VALIDATE_NAME,
                    null
                );
                tables = Lists2.map(
                    restoreSnapshot.tables(),
                    table -> table.map(x -> exprAnalyzerWithFieldsAsString.convert(x, exprCtx))
                );
            }
            case CUSTOM -> {
                for (String typeName : restoreSnapshot.types()) {
                    typeName = typeName.toUpperCase(Locale.ENGLISH);
                    if (typeName.equals("TABLES")) {
                        includeTables = true;
                    } else if (typeName.equals("ANALYZERS")) {
                        // custom analyzers are stored inside persistent cluster settings
                        globalSettings.add(AnalyzerSettings.CUSTOM_ANALYSIS_SETTINGS_PREFIX);
                        includeGlobalSettings = true;
                    } else {
                        var customTypes = METADATA_CUSTOM_TYPE_MAP.get(typeName);
                        if (customTypes == null) {
                            throw new IllegalArgumentException("Unknown metadata type '" + typeName + "'");
                        }
                        if ("USERS".equals(typeName) || "PRIVILEGES".equals(typeName)) {
                            DEPRECATION_LOGGER.deprecatedAndMaybeLog("restore_snapshot.metadata",
                                typeName + " keyword is deprecated, please use USERMANAGEMENT instead");
                        }
                        includeCustomMetadata = true;
                        customMetadataTypes.addAll(customTypes);
                    }
                }
            }
            default -> throw new AssertionError("Unsupported restore mode='" + restoreSnapshot.mode() + "'");
        }

        return new AnalyzedRestoreSnapshot(
            repositoryName,
            snapshotName,
            tables,
            includeTables,
            includeCustomMetadata,
            customMetadataTypes,
            includeGlobalSettings,
            globalSettings,
            properties
        );
    }
}
