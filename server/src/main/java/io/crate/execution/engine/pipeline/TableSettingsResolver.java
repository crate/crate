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

package io.crate.execution.engine.pipeline;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;

import io.crate.exceptions.RelationUnknown;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

public final class TableSettingsResolver {

    public static Settings get(Metadata metadata, RelationName relationName, boolean partitioned) {
        if (partitioned) {
            return forPartitionedTable(metadata, relationName);
        }
        return forTable(metadata, relationName);
    }

    private static Settings forTable(Metadata metadata, RelationName relationName) {
        IndexMetadata indexMetadata = metadata.index(relationName.indexNameOrAlias());
        if (indexMetadata == null) {
            throw new IndexNotFoundException(relationName.indexNameOrAlias());
        }
        return indexMetadata.getSettings();
    }

    private static Settings forPartitionedTable(Metadata metadata, RelationName relationName) {
        String templateName = PartitionName.templateName(relationName.schema(), relationName.name());
        IndexTemplateMetadata templateMetadata = metadata.templates().get(templateName);
        if (templateMetadata == null) {
            throw new RelationUnknown(relationName);
        }
        return templateMetadata.settings();
    }

    private TableSettingsResolver() {
    }
}
