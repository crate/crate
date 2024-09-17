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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.AliasValidator;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexTemplateException;

import io.crate.Constants;
import io.crate.execution.ddl.tables.CreateTableRequest;
import io.crate.execution.ddl.tables.MappingUtil;
import io.crate.metadata.DocReferences;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;

public final class Templates {

    private static final Logger LOGGER = LogManager.getLogger(Templates.class);

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

    public static ClusterState add(IndicesService indicesService,
                                   MetadataCreateIndexService metadataIndexService,
                                   ClusterState clusterState,
                                   CreateTableRequest request,
                                   Settings settings) throws Exception {
        RelationName relationName = request.getTableName();
        String templateName = PartitionName.templateName(relationName.schema(), relationName.name());
        String templatePrefix = PartitionName.templatePrefix(relationName.schema(), relationName.name());
        Alias alias = new Alias(relationName.indexNameOrAlias());

        validate(metadataIndexService, templateName, templatePrefix, request.settings(), alias);

        final String temporaryIndexName = UUIDs.randomBase64UUID();

        // use the provided values, otherwise just pick valid dummy values
        int dummyPartitionSize = IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING.get(settings);
        int dummyShards = settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS,
                dummyPartitionSize == 1 ? 1 : dummyPartitionSize + 1);

        // create index service for parsing and validating "mappings"
        Settings dummySettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(settings)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, dummyShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .build();

        final IndexMetadata tmpIndexMetadata = IndexMetadata.builder(temporaryIndexName)
            .settings(dummySettings)
            .build();
        return indicesService.withTempIndexService(tmpIndexMetadata, indexService -> {
            Version versionCreated = Version.min(
                Version.CURRENT,
                clusterState.nodes().getSmallestNonClientNodeVersion()
            );
            Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata());
            IndexTemplateMetadata.Builder templateBuilder = IndexTemplateMetadata.builder(templateName)
                .version(null)
                .patterns(List.of(templatePrefix))
                .putAlias(new AliasMetadata(alias.name()))
                .settings(Settings.builder()
                    .put(settings)
                    .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), versionCreated));

            List<Reference> references = DocReferences.applyOid(
                request.references(),
                metadataBuilder.columnOidSupplier()
            );
            var mapping = Map.of(
                Constants.DEFAULT_MAPPING_TYPE,
                MappingUtil.createMapping(
                    MappingUtil.AllocPosition.forNewTable(),
                    request.pkConstraintName(),
                    references,
                    request.pKeyIndices(),
                    request.checkConstraints(),
                    request.partitionedBy(),
                    request.tableColumnPolicy(),
                    request.routingColumn()
                )
            );
            try {
                templateBuilder
                    .putMapping(new CompressedXContent(Strings.toString(JsonXContent.builder().map(mapping))));
            } catch (Exception e) {
                throw new MapperParsingException("Failed to parse mapping: {}", e, e.getMessage());
            }
            LOGGER.info("adding template [{}] for index pattern {}", templateName, templatePrefix);
            return ClusterState.builder(clusterState)
                .metadata(metadataBuilder.put(templateBuilder))
                .build();
        });
    }


    private static void validate(MetadataCreateIndexService metadataIndexService,
                                 String templateName,
                                 String templatePrefix,
                                 Settings settings,
                                 Alias alias) {
        List<String> validationErrors = new ArrayList<>();
        if (templateName.contains(" ")) {
            validationErrors.add("name must not contain a space");
        }
        if (templateName.contains(",")) {
            validationErrors.add("name must not contain a ','");
        }
        if (templateName.contains("#")) {
            validationErrors.add("name must not contain a '#'");
        }
        if (templatePrefix.contains(" ")) {
            validationErrors.add("template must not contain a space");
        }
        if (templatePrefix.contains(",")) {
            validationErrors.add("template must not contain a ','");
        }
        if (templatePrefix.contains("#")) {
            validationErrors.add("template must not contain a '#'");
        }
        if (!Strings.validFileNameExcludingAstrix(templatePrefix)) {
            validationErrors.add("template must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
        }

        List<String> indexSettingsValidation = metadataIndexService.getIndexSettingsValidationErrors(settings, true);
        validationErrors.addAll(indexSettingsValidation);
        if (!validationErrors.isEmpty()) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationErrors(validationErrors);
            throw new InvalidIndexTemplateException(templateName, validationException.getMessage());
        }

        //we validate the alias only partially, as we don't know yet to which index it'll get applied to
        AliasValidator.validateAliasName(alias.name());
        if (templatePrefix.equals(alias.name())) {
            throw new IllegalArgumentException("Alias [" + alias.name() +
                "] cannot be the same as template prefix [" + templatePrefix + "]");
        }
    }
}
