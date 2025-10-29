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

package io.crate.execution.ddl;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.common.compress.CompressedXContent;

import io.crate.Constants;
import io.crate.execution.ddl.tables.MappingUtil;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

public final class Templates {

    private static final Logger LOGGER = LogManager.getLogger(Templates.class);

    public static IndexTemplateMetadata of(RelationMetadata.Table table) {
        assert table.partitionedBy().isEmpty() == false : "table must be partitioned";
        RelationName relationName = table.name();
        String templateName = PartitionName.templateName(relationName.schema(), relationName.name());
        String templatePrefix = PartitionName.templatePrefix(relationName.schema(), relationName.name());
        return IndexTemplateMetadata.builder(templateName)
            .version(null)
            .patterns(List.of(templatePrefix))
            .putAlias(new AliasMetadata(relationName.indexNameOrAlias()))
            .settings(table.settings())
            .putMapping(new CompressedXContent(Map.of(
                Constants.DEFAULT_MAPPING_TYPE,
                MappingUtil.createMapping(
                    MappingUtil.AllocPosition.forExistingRefs(),
                    table.pkConstraintName(),
                    table.columns(),
                    table.primaryKeys(),
                    table.checkConstraints(),
                    table.partitionedBy(),
                    table.columnPolicy(),
                    table.routingColumn()
                )
            )))
            .build();
    }

    public static IndexTemplateMetadata.Builder withName(IndexTemplateMetadata source, RelationName newName) {
        String targetTemplateName = PartitionName.templateName(newName.schema(), newName.name());
        String targetAlias = newName.indexNameOrAlias();
        IndexTemplateMetadata.Builder templateBuilder = IndexTemplateMetadata
            .builder(targetTemplateName)
            .patterns(Collections.singletonList(PartitionName.templatePrefix(newName.schema(), newName.name())))
            .settings(source.settings())
            .putMapping(source.mapping())
            .putAlias(new AliasMetadata(targetAlias))
            .version(source.version());

        return templateBuilder;
    }
}
