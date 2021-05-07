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

import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BoundRestoreSnapshot {

    private final String repository;
    private final String snapshot;
    private final HashSet<RestoreTableInfo> restoreTables;
    private final boolean includeTables;
    private final boolean includeCustomMetadata;
    private final Set<String> customMetadataTypes;
    private final boolean includeGlobalSettings;
    private final List<String> globalSettings;
    private final Settings settings;

    public BoundRestoreSnapshot(String repository,
                                String snapshot,
                                HashSet<RestoreTableInfo> restoreTables,
                                boolean includeTables,
                                boolean includeCustomMetadata,
                                Set<String> customMetadataTypes,
                                boolean includeGlobalSettings,
                                List<String> globalSettings,
                                Settings settings) {
        this.repository = repository;
        this.snapshot = snapshot;
        this.restoreTables = restoreTables;
        this.includeTables = includeTables;
        this.includeCustomMetadata = includeCustomMetadata;
        this.customMetadataTypes = customMetadataTypes;
        this.includeGlobalSettings = includeGlobalSettings;
        this.globalSettings = globalSettings;
        this.settings = settings;
    }

    public String snapshot() {
        return snapshot;
    }

    public String repository() {
        return repository;
    }

    public Settings settings() {
        return settings;
    }

    public Set<RestoreTableInfo> restoreTables() {
        return restoreTables;
    }

    public boolean includeTables() {
        return includeTables;
    }

    public boolean includeCustomMetadata() {
        return includeCustomMetadata;
    }

    public Set<String> customMetadataTypes() {
        return customMetadataTypes;
    }

    public boolean includeGlobalSettings() {
        return includeGlobalSettings;
    }

    public List<String> globalSettings() {
        return globalSettings;
    }

    public static class RestoreTableInfo {

        private final RelationName relationName;
        private final PartitionName partitionName;
        private final String partitionTemplate;

        public RestoreTableInfo(RelationName relationName, @Nullable PartitionName partitionName) {
            this.relationName = relationName;
            this.partitionName = partitionName;
            this.partitionTemplate = PartitionName.templateName(relationName.schema(), tableIdent().name());
        }

        public RelationName tableIdent() {
            return relationName;
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
