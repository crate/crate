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

import com.google.common.collect.ImmutableList;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

public class RestoreSnapshotAnalyzedStatement implements DDLStatement {

    private static final List<RestoreTableInfo> ALL_TABLES = ImmutableList.of();

    private final String snapshotName;
    private final String repositoryName;
    private final Settings settings;

    private final List<RestoreTableInfo> tables;

    private RestoreSnapshotAnalyzedStatement(String snapshotName, String repositoryName, Settings settings, List<RestoreTableInfo> restoreTables) {
        this.snapshotName = snapshotName;
        this.repositoryName = repositoryName;
        this.settings = settings;
        this.tables = restoreTables;
    }

    public static RestoreSnapshotAnalyzedStatement forTables(String snapshotName, String repositoryName, Settings settings, List<RestoreTableInfo> restoreTables) {
        return new RestoreSnapshotAnalyzedStatement(snapshotName, repositoryName, settings, restoreTables);
    }

    public static RestoreSnapshotAnalyzedStatement all(String snapshotName, String repositoryName, Settings settings) {
        return new RestoreSnapshotAnalyzedStatement(snapshotName, repositoryName, settings, ALL_TABLES);
    }

    public String snapshotName() {
        return snapshotName;
    }

    public String repositoryName() {
        return repositoryName;
    }

    public Settings settings() {
        return settings;
    }

    public List<RestoreTableInfo> restoreTables() {
        return tables;
    }

    public boolean restoreAll() {
        return tables == ALL_TABLES;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitRestoreSnapshotAnalyzedStatement(this, context);
    }

    public static class RestoreTableInfo {

        private final TableIdent tableIdent;
        private final PartitionName partitionName;
        private final String partitionTemplate;

        public RestoreTableInfo(TableIdent tableIdent, @Nullable PartitionName partitionName) {
            this.tableIdent = tableIdent;
            this.partitionName = partitionName;
            this.partitionTemplate = PartitionName.templateName(tableIdent.schema(), tableIdent().name());
        }

        public TableIdent tableIdent() {
            return tableIdent;
        }

        @Nullable
        public PartitionName partitionName() {
            return partitionName;
        }

        public boolean hasPartitionInfo() {
            return partitionName != null;
        }

        public String partitionTemplate() {
            return partitionTemplate;
        }
    }
}
