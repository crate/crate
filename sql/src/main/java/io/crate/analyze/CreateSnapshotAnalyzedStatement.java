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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.Snapshot;

import java.util.List;

public class CreateSnapshotAnalyzedStatement implements DDLStatement {

    public static final List<String> ALL_INDICES = ImmutableList.of("_all");

    private final Snapshot snapshot;
    private final Settings snapshotSettings;

    private final boolean includeMetadata;
    private final List<String> indices;

    private CreateSnapshotAnalyzedStatement(Snapshot snapshot,
                                            Settings snapshotSettings,
                                            List<String> indices,
                                            boolean includeMetadata) {
        this.snapshot = snapshot;
        this.snapshotSettings = snapshotSettings;
        this.indices = indices;
        this.includeMetadata = includeMetadata;
    }

    public static CreateSnapshotAnalyzedStatement forTables(Snapshot snapshot, Settings snapshotSettings, List<String> indices, boolean includeMetadata) {
        return new CreateSnapshotAnalyzedStatement(snapshot, snapshotSettings, indices, includeMetadata);
    }

    public static CreateSnapshotAnalyzedStatement all(Snapshot snapshot, Settings snapshotSettings) {
        return new CreateSnapshotAnalyzedStatement(snapshot, snapshotSettings, ALL_INDICES, true);
    }

    public Snapshot snapshot() {
        return snapshot;
    }

    public Settings snapshotSettings() {
        return snapshotSettings;
    }

    public List<String> indices() {
        return indices;
    }

    public boolean isAllSnapshot() {
        return (indices == ALL_INDICES) && includeMetadata;
    }

    public boolean isNoOp() {
        return indices.isEmpty() && !includeMetadata;
    }

    public boolean includeMetadata() {
        return includeMetadata;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitCreateSnapshotAnalyzedStatement(this, context);
    }
}
