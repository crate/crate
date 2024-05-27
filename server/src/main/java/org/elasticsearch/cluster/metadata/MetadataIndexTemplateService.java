/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.metadata;

import static org.elasticsearch.cluster.metadata.MetadataCreateIndexService.setIndexVersionCreatedSetting;
import static org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.NO_LONGER_ASSIGNED;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexTemplateException;

import io.crate.Constants;
import io.crate.common.unit.TimeValue;
import io.crate.execution.ddl.tables.CreateTableRequest;
import io.crate.execution.ddl.tables.CreateTableResponse;
import io.crate.execution.ddl.tables.MappingUtil;
import io.crate.metadata.DocReferences;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;

/**
 * Service responsible for creating templates
 */
public class MetadataIndexTemplateService {

    private static final Logger LOGGER = LogManager.getLogger(MetadataIndexTemplateService.class);

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final MetadataCreateIndexService metadataCreateIndexService;
    private final IndexScopedSettings indexScopedSettings;
    private final NamedXContentRegistry xContentRegistry;

    @Inject
    public MetadataIndexTemplateService(ClusterService clusterService,
                                        MetadataCreateIndexService metadataCreateIndexService,
                                        IndicesService indicesService,
                                        IndexScopedSettings indexScopedSettings,
                                        NamedXContentRegistry xContentRegistry) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.metadataCreateIndexService = metadataCreateIndexService;
        this.indexScopedSettings = indexScopedSettings;
        this.xContentRegistry = xContentRegistry;
    }

    public void putTemplate(CreateTableRequest request,
                            ActionListener<CreateTableResponse> listener) {
        Settings.Builder updatedSettingsBuilder = Settings.builder();
        updatedSettingsBuilder.put(request.settings()).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX);
        Settings settings = updatedSettingsBuilder.build();
        indexScopedSettings.validate(settings, true);

        RelationName relationName = request.getTableName();
        String templateName = PartitionName.templateName(relationName.schema(), relationName.name());
        String templatePrefix = PartitionName.templatePrefix(relationName.schema(), relationName.name());
        Alias alias = new Alias(relationName.indexNameOrAlias());

        try {
            validate(templateName, templatePrefix, settings, alias);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        final IndexTemplateMetadata.Builder templateBuilder = IndexTemplateMetadata.builder(templateName);

        clusterService.submitStateUpdateTask(
            "create-index-template [" + templateName + "], cause [create table]",
            new ClusterStateUpdateTask(Priority.URGENT) {

                @Override
                public TimeValue timeout() {
                    return request.masterNodeTimeout();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    if (currentState.metadata().templates().containsKey(templateName)) {
                        throw new IllegalArgumentException("index_template [" + templateName + "] already exists");
                    }
                    Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                    validateAndAddTemplate(
                        request,
                        templatePrefix,
                        settings,
                        metadataBuilder,
                        templateBuilder,
                        indicesService,
                        xContentRegistry,
                        currentState
                    );

                    templateBuilder.putAlias(new AliasMetadata(alias.name()));
                    metadataBuilder.put(templateBuilder);

                    LOGGER.info("adding template [{}] for index pattern {}", templateName, templatePrefix);
                    return ClusterState.builder(currentState).metadata(metadataBuilder).build();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(new CreateTableResponse(true));
                }
            }
        );
    }

    private static void validateAndAddTemplate(CreateTableRequest createTableRequest,
                                               String templatePrefix,
                                               Settings settings,
                                               Metadata.Builder metadataBuilder,
                                               IndexTemplateMetadata.Builder templateBuilder,
                                               IndicesService indicesService,
                                               NamedXContentRegistry xContentRegistry,
                                               ClusterState currentState) throws Exception {
        Index createdIndex = null;
        final String temporaryIndexName = UUIDs.randomBase64UUID();
        try {
            // use the provided values, otherwise just pick valid dummy values
            int dummyPartitionSize = IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING.get(settings);
            int dummyShards = settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS,
                    dummyPartitionSize == 1 ? 1 : dummyPartitionSize + 1);

            //create index service for parsing and validating "mappings"
            Settings dummySettings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(settings)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, dummyShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                .build();

            final IndexMetadata tmpIndexMetadata = IndexMetadata.builder(temporaryIndexName).settings(dummySettings).build();
            IndexService dummyIndexService = indicesService.createIndex(tmpIndexMetadata, Collections.emptyList(), false);
            createdIndex = dummyIndexService.index();

            templateBuilder.version(null);
            templateBuilder.patterns(List.of(templatePrefix));

            // inject `index.version.created` to the template settings to flag version of template creation (partitioned table)
            var templateSettingsBuilder = Settings.builder().put(settings);
            setIndexVersionCreatedSetting(templateSettingsBuilder, currentState);

            templateBuilder.settings(templateSettingsBuilder.build());
            List<Reference> references = DocReferences.applyOid(createTableRequest.references(), metadataBuilder.columnOidSupplier());
            var mapping = MappingUtil.createMapping(
                MappingUtil.AllocPosition.forNewTable(),
                createTableRequest.pkConstraintName(),
                references,
                createTableRequest.pKeyIndices(),
                createTableRequest.checkConstraints(),
                createTableRequest.partitionedBy(),
                createTableRequest.tableColumnPolicy(),
                createTableRequest.routingColumn()
            );
            mapping = Map.of(Constants.DEFAULT_MAPPING_TYPE, mapping); // We used PutIndexTemplateRequest.mapping which wraps mapping with default type
            try {
                templateBuilder.putMapping(new CompressedXContent(Strings.toString(JsonXContent.builder().map(mapping))));
            } catch (Exception e) {
                throw new MapperParsingException("Failed to parse mapping: {}", e, e.getMessage());
            }
        } finally {
            if (createdIndex != null) {
                indicesService.removeIndex(createdIndex, NO_LONGER_ASSIGNED, " created for parsing template mapping");
            }
        }
    }

    private void validate(String templateName, String templatePrefix, Settings settings, Alias alias) {
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

        try {
            indexScopedSettings.validate(settings, true); // templates must be consistent with regards to dependencies
        } catch (IllegalArgumentException iae) {
            validationErrors.add(iae.getMessage());
            for (Throwable t : iae.getSuppressed()) {
                validationErrors.add(t.getMessage());
            }
        }
        List<String> indexSettingsValidation = metadataCreateIndexService.getIndexSettingsValidationErrors(settings, true);
        validationErrors.addAll(indexSettingsValidation);
        if (!validationErrors.isEmpty()) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationErrors(validationErrors);
            throw new InvalidIndexTemplateException(templateName, validationException.getMessage());
        }

        //we validate the alias only partially, as we don't know yet to which index it'll get applied to
        AliasValidator.validateAliasStandalone(alias);
        if (templatePrefix.equals(alias.name())) {
            throw new IllegalArgumentException("Alias [" + alias.name() +
                "] cannot be the same as template prefix [" + templatePrefix + "]");
        }
    }
}
