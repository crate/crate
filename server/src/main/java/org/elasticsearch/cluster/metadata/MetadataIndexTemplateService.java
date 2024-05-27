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
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.support.master.MasterNodeRequest;
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
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.jetbrains.annotations.Nullable;

import io.crate.Constants;
import io.crate.common.unit.TimeValue;
import io.crate.execution.ddl.tables.CreateTableRequest;
import io.crate.execution.ddl.tables.MappingUtil;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Reference;

/**
 * Service responsible for submitting index templates updates
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

    public void putTemplate(final PutRequest request, @Nullable final CreateTableRequest createTableRequest, final PutListener listener) {
        Settings.Builder updatedSettingsBuilder = Settings.builder();
        updatedSettingsBuilder.put(request.settings).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX);
        request.settings(updatedSettingsBuilder.build());

        if (request.name == null) {
            listener.onFailure(new IllegalArgumentException("index_template must provide a name"));
            return;
        }
        if (request.indexPatterns == null) {
            listener.onFailure(new IllegalArgumentException("index_template must provide a template"));
            return;
        }

        try {
            validate(request);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        final IndexTemplateMetadata.Builder templateBuilder = IndexTemplateMetadata.builder(request.name);

        clusterService.submitStateUpdateTask(
            "create-index-template [" + request.name + "], cause [" + request.cause + "]",
            new ClusterStateUpdateTask(Priority.URGENT) {

                @Override
                public TimeValue timeout() {
                    return request.masterTimeout;
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    if (request.create && currentState.metadata().templates().containsKey(request.name)) {
                        throw new IllegalArgumentException("index_template [" + request.name + "] already exists");
                    }
                    Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                    validateAndAddTemplate(request, createTableRequest, metadataBuilder, templateBuilder, indicesService, xContentRegistry, currentState);

                    for (Alias alias : request.aliases) {
                        AliasMetadata aliasMetadata = new AliasMetadata(alias.name());
                        templateBuilder.putAlias(aliasMetadata);
                    }
                    IndexTemplateMetadata template = templateBuilder.build();

                    metadataBuilder.put(template);

                    LOGGER.info("adding template [{}] for index patterns {}", request.name, request.indexPatterns);
                    return ClusterState.builder(currentState).metadata(metadataBuilder).build();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(new PutResponse(true, templateBuilder.build()));
                }
            }
        );
    }

    private static void validateAndAddTemplate(final PutRequest request,
                                               @Nullable final CreateTableRequest createTableRequest,
                                               Metadata.Builder metadataBuilder,
                                               IndexTemplateMetadata.Builder templateBuilder,
                                               IndicesService indicesService,
                                               NamedXContentRegistry xContentRegistry,
                                               ClusterState currentState) throws Exception {
        Index createdIndex = null;
        final String temporaryIndexName = UUIDs.randomBase64UUID();
        try {
            // use the provided values, otherwise just pick valid dummy values
            int dummyPartitionSize = IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING.get(request.settings);
            int dummyShards = request.settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS,
                    dummyPartitionSize == 1 ? 1 : dummyPartitionSize + 1);

            //create index service for parsing and validating "mappings"
            Settings dummySettings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(request.settings)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, dummyShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                .build();

            final IndexMetadata tmpIndexMetadata = IndexMetadata.builder(temporaryIndexName).settings(dummySettings).build();
            IndexService dummyIndexService = indicesService.createIndex(tmpIndexMetadata, Collections.emptyList(), false);
            createdIndex = dummyIndexService.index();

            templateBuilder.version(request.version);
            templateBuilder.patterns(request.indexPatterns);

            // inject `index.version.created` to the template settings to flag version of template creation (partitioned table)
            var templateSettingsBuilder = Settings.builder().put(request.settings);
            setIndexVersionCreatedSetting(templateSettingsBuilder, currentState);

            templateBuilder.settings(templateSettingsBuilder.build());
            if (createTableRequest != null) {
                // New code path where we create mapping at the last stage and assign OID in the same cluster state update with template creation.
                List<Reference> references = DocReferences.applyOid(
                        createTableRequest.references(),
                        metadataBuilder.columnOidSupplier()
                );
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
                dummyIndexService.mapperService().merge(mapping, MergeReason.MAPPING_UPDATE);
            } else {
                // BWC code path. Do not remove in 5.5 or later since it's used on upgrade as well.
                if (request.mapping != null) {
                    try {
                        templateBuilder.putMapping(request.mapping);
                    } catch (Exception e) {
                        throw new MapperParsingException("Failed to parse mapping: {}", e, e.getMessage());
                    }
                    dummyIndexService.mapperService().merge(MapperService.parseMapping(xContentRegistry, request.mapping), MergeReason.MAPPING_UPDATE);
                }
            }
        } finally {
            if (createdIndex != null) {
                indicesService.removeIndex(createdIndex, NO_LONGER_ASSIGNED, " created for parsing template mapping");
            }
        }
    }

    private void validate(PutRequest request) {
        List<String> validationErrors = new ArrayList<>();
        if (request.name.contains(" ")) {
            validationErrors.add("name must not contain a space");
        }
        if (request.name.contains(",")) {
            validationErrors.add("name must not contain a ','");
        }
        if (request.name.contains("#")) {
            validationErrors.add("name must not contain a '#'");
        }
        for (String indexPattern : request.indexPatterns) {
            if (indexPattern.contains(" ")) {
                validationErrors.add("template must not contain a space");
            }
            if (indexPattern.contains(",")) {
                validationErrors.add("template must not contain a ','");
            }
            if (indexPattern.contains("#")) {
                validationErrors.add("template must not contain a '#'");
            }
            if (!Strings.validFileNameExcludingAstrix(indexPattern)) {
                validationErrors.add("template must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
            }
        }

        try {
            indexScopedSettings.validate(request.settings, true); // templates must be consistent with regards to dependencies
        } catch (IllegalArgumentException iae) {
            validationErrors.add(iae.getMessage());
            for (Throwable t : iae.getSuppressed()) {
                validationErrors.add(t.getMessage());
            }
        }
        List<String> indexSettingsValidation = metadataCreateIndexService.getIndexSettingsValidationErrors(request.settings, true);
        validationErrors.addAll(indexSettingsValidation);
        if (!validationErrors.isEmpty()) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationErrors(validationErrors);
            throw new InvalidIndexTemplateException(request.name, validationException.getMessage());
        }

        for (Alias alias : request.aliases) {
            //we validate the alias only partially, as we don't know yet to which index it'll get applied to
            AliasValidator.validateAliasStandalone(alias);
            if (request.indexPatterns.contains(alias.name())) {
                throw new IllegalArgumentException("Alias [" + alias.name() +
                    "] cannot be the same as any pattern in [" + String.join(", ", request.indexPatterns) + "]");
            }
        }
    }

    public interface PutListener {

        void onResponse(PutResponse response);

        void onFailure(Exception e);
    }

    public static class PutRequest {
        final String name;
        final String cause;
        boolean create;
        Integer version;
        List<String> indexPatterns;
        Settings settings = Settings.Builder.EMPTY_SETTINGS;
        String mapping;
        List<Alias> aliases = new ArrayList<>();

        TimeValue masterTimeout = MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT;

        public PutRequest(String cause, String name) {
            this.cause = cause;
            this.name = name;
        }

        public PutRequest patterns(List<String> indexPatterns) {
            this.indexPatterns = indexPatterns;
            return this;
        }

        public PutRequest create(boolean create) {
            this.create = create;
            return this;
        }

        public PutRequest settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public PutRequest mapping(String mapping) {
            this.mapping = mapping;
            return this;
        }

        public PutRequest aliases(Set<Alias> aliases) {
            this.aliases.addAll(aliases);
            return this;
        }

        public PutRequest masterTimeout(TimeValue masterTimeout) {
            this.masterTimeout = masterTimeout;
            return this;
        }

        public PutRequest version(Integer version) {
            this.version = version;
            return this;
        }
    }

    public static class PutResponse {
        private final boolean acknowledged;
        private final IndexTemplateMetadata template;

        public PutResponse(boolean acknowledged, IndexTemplateMetadata template) {
            this.acknowledged = acknowledged;
            this.template = template;
        }

        public boolean acknowledged() {
            return acknowledged;
        }

        public IndexTemplateMetadata template() {
            return template;
        }
    }
}
