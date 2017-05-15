/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.SettingsApplier;
import io.crate.metadata.settings.SettingsAppliers;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.user.User;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.OptimizeStatement;
import io.crate.sql.tree.Table;
import org.elasticsearch.common.settings.Settings;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.crate.analyze.OptimizeSettings.*;

class OptimizeTableAnalyzer {

    private static final ImmutableMap<String, SettingsApplier> SETTINGS = ImmutableMap.<String, SettingsApplier>builder()
        .put(MAX_NUM_SEGMENTS.name(), new SettingsAppliers.IntSettingsApplier(MAX_NUM_SEGMENTS))
        .put(ONLY_EXPUNGE_DELETES.name(), new SettingsAppliers.BooleanSettingsApplier(ONLY_EXPUNGE_DELETES))
        .put(FLUSH.name(), new SettingsAppliers.BooleanSettingsApplier(FLUSH))
        .put(UPGRADE_SEGMENTS.name(), new SettingsAppliers.BooleanSettingsApplier(UPGRADE_SEGMENTS))
        .build();

    private final Schemas schemas;

    OptimizeTableAnalyzer(Schemas schemas) {
        this.schemas = schemas;
    }

    public OptimizeTableAnalyzedStatement analyze(OptimizeStatement stmt, Analysis analysis) {
        Set<String> indexNames = getIndexNames(
            stmt.tables(),
            schemas,
            analysis.parameterContext(),
            analysis.sessionContext().defaultSchema(),
            analysis.sessionContext().user());

        // validate and extract settings
        Settings.Builder builder = GenericPropertiesConverter.settingsFromProperties(
            stmt.properties(), analysis.parameterContext(), SETTINGS);
        Settings settings = builder.build();
        validateSettings(settings, stmt.properties());
        return new OptimizeTableAnalyzedStatement(indexNames, settings);
    }

    private static Set<String> getIndexNames(List<Table> tables,
                                             Schemas schemas,
                                             ParameterContext parameterContext,
                                             String defaultSchema,
                                             User user) {
        Set<String> indexNames = new HashSet<>(tables.size());
        for (Table nodeTable : tables) {
            TableInfo tableInfo = schemas.getTableInfo(TableIdent.of(nodeTable, defaultSchema), Operation.OPTIMIZE, user);
            if (tableInfo instanceof BlobTableInfo) {
                indexNames.add(((BlobTableInfo) tableInfo).concreteIndex());
            } else {
                indexNames.addAll(TableAnalyzer.filteredIndices(
                    parameterContext,
                    nodeTable.partitionProperties(), (DocTableInfo) tableInfo));
            }
        }
        return indexNames;
    }

    private void validateSettings(Settings settings, Optional<GenericProperties> stmtParameters) {
        if (settings.getAsBoolean(UPGRADE_SEGMENTS.name(), UPGRADE_SEGMENTS.defaultValue())
            && stmtParameters.get().size() > 1) {
            throw new IllegalArgumentException("cannot use other parameters if " +
                                               UPGRADE_SEGMENTS.name() + " is set to true");
        }
    }
}
