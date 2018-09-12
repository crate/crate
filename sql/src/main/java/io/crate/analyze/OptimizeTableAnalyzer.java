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
import io.crate.metadata.SearchPath;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.OptimizeStatement;
import io.crate.sql.tree.Table;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class OptimizeTableAnalyzer {

    public static final Setting<Integer> MAX_NUM_SEGMENTS =
        Setting.intSetting(
            "max_num_segments",
            ForceMergeRequest.Defaults.MAX_NUM_SEGMENTS,
            ForceMergeRequest.Defaults.MAX_NUM_SEGMENTS,
            Integer.MAX_VALUE);

    public static final Setting<Boolean> ONLY_EXPUNGE_DELETES =
        Setting.boolSetting("only_expunge_deletes", ForceMergeRequest.Defaults.ONLY_EXPUNGE_DELETES);

    public static final Setting<Boolean> FLUSH = Setting.boolSetting("flush", ForceMergeRequest.Defaults.FLUSH);

    public static final Setting<Boolean> UPGRADE_SEGMENTS = Setting.boolSetting("upgrade_segments", false);

    private static final ImmutableMap<String, Setting> SETTINGS = ImmutableMap.<String, Setting>builder()
        .put(MAX_NUM_SEGMENTS.getKey(), MAX_NUM_SEGMENTS)
        .put(ONLY_EXPUNGE_DELETES.getKey(), ONLY_EXPUNGE_DELETES)
        .put(FLUSH.getKey(), FLUSH)
        .put(UPGRADE_SEGMENTS.getKey(), UPGRADE_SEGMENTS)
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
            analysis.sessionContext().searchPath());

        // validate and extract settings
        Settings.Builder builder = GenericPropertiesConverter.settingsFromProperties(
            stmt.properties(), analysis.parameterContext().parameters(), SETTINGS);
        Settings settings = builder.build();
        validateSettings(settings, stmt.properties());
        return new OptimizeTableAnalyzedStatement(indexNames, settings);
    }

    private static Set<String> getIndexNames(List<Table> tables,
                                             Schemas schemas,
                                             ParameterContext parameterContext,
                                             SearchPath searchPath) {
        Set<String> indexNames = new HashSet<>(tables.size());
        for (Table nodeTable : tables) {
            TableInfo tableInfo = schemas.resolveTableInfo(nodeTable.getName(), Operation.OPTIMIZE, searchPath);
            if (tableInfo instanceof BlobTableInfo) {
                indexNames.add(((BlobTableInfo) tableInfo).concreteIndices()[0]);
            } else {
                indexNames.addAll(TableAnalyzer.filteredIndices(
                    parameterContext,
                    nodeTable.partitionProperties(), (DocTableInfo) tableInfo));
            }
        }
        return indexNames;
    }

    private void validateSettings(Settings settings, GenericProperties stmtParameters) {
        if (UPGRADE_SEGMENTS.get(settings) && stmtParameters.size() > 1) {
            throw new IllegalArgumentException("cannot use other parameters if " +
                                               UPGRADE_SEGMENTS.getKey() + " is set to true");
        }
    }
}
