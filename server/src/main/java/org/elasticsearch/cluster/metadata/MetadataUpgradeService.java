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

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_UPGRADED;
import static org.elasticsearch.cluster.metadata.Metadata.Builder.NO_OID_COLUMN_OID_SUPPLIER;
import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.expression.udf.UserDefinedFunctionsMetadata;
import io.crate.metadata.IndexName;
import io.crate.metadata.IndexParts;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.DocTableInfoFactory;
import io.crate.metadata.upgrade.IndexTemplateUpgrader;
import io.crate.metadata.upgrade.MetadataIndexUpgrader;
import io.crate.sql.tree.CheckConstraint;

/**
 * This service is responsible for upgrading legacy index metadata to the current version
 * <p>
 * Every time an existing index is introduced into cluster this service should be used
 * to upgrade the existing index metadata to the latest version of the cluster. It typically
 * occurs during cluster upgrade, when dangling indices are imported into the cluster or indices
 * are restored from a repository.
 */
public class MetadataUpgradeService {

    private static final Logger LOGGER = LogManager.getLogger(MetadataUpgradeService.class);

    private final IndexScopedSettings indexScopedSettings;
    private final MetadataIndexUpgrader indexUpgrader;
    private final DocTableInfoFactory tableFactory;
    private final UserDefinedFunctionService userDefinedFunctionService;
    private final IndexTemplateUpgrader templateUpgrader;

    public MetadataUpgradeService(NodeContext nodeContext,
                                  IndexScopedSettings indexScopedSettings,
                                  UserDefinedFunctionService userDefinedFunctionService) {
        this.tableFactory = new DocTableInfoFactory(nodeContext);
        this.indexScopedSettings = indexScopedSettings;
        this.indexUpgrader = new MetadataIndexUpgrader();
        this.templateUpgrader = new IndexTemplateUpgrader();
        this.userDefinedFunctionService = userDefinedFunctionService;
    }

    /**
     * Elasticsearch 2.0 removed several deprecated features and as well as support for Lucene 3.x. This method calls
     * {@link MetadataUpgradeService} to makes sure that indices are compatible with the current version. The
     * MetadataIndexUpgradeService might also update obsolete settings if needed.
     *
     * @return input <code>metadata</code> if no upgrade is needed or an upgraded metadata
     */
    public Metadata upgradeMetadata(Metadata metadata) {
        boolean changed = false;
        final Metadata.Builder upgradedMetadata = Metadata.builder(metadata);

        // carries upgraded IndexTemplateMetadata to MetadataIndexUpgradeService.upgradeIndexMetadata
        final Map<String, IndexTemplateMetadata> upgradedIndexTemplateMetadata = new HashMap<>();

        // upgrade current templates
        if (applyUpgrader(
            metadata.templates(),
            this::upgradeTemplates,
            upgradedMetadata::removeTemplate,
            (s, indexTemplateMetadata) -> {
                upgradedIndexTemplateMetadata.put(s, indexTemplateMetadata);
                upgradedMetadata.put(indexTemplateMetadata);
            })) {
            changed = true;
        }

        // upgrade index meta data
        for (IndexMetadata indexMetadata : metadata) {
            String indexName = indexMetadata.getIndex().getName();
            IndexMetadata newMetadata = upgradeIndexMetadata(
                indexMetadata,
                IndexName.isPartitioned(indexName) ?
                    upgradedIndexTemplateMetadata.get(PartitionName.templateName(indexName)) :
                    null,
                Version.CURRENT.minimumIndexCompatibilityVersion(),
                metadata.custom(UserDefinedFunctionsMetadata.TYPE));
            changed |= indexMetadata != newMetadata;
            // Remove any existing metadata, registered by it's name, for the index
            if (upgradedMetadata.get(indexName) != null) {
                changed = true;
                upgradedMetadata.remove(indexName);
            }
            upgradedMetadata.put(newMetadata, false);
        }

        return changed
            ? addOrUpgradeRelationMetadata(upgradedMetadata.build())
            : addOrUpgradeRelationMetadata(metadata);
    }

    private static <Data> boolean applyUpgrader(ImmutableOpenMap<String, Data> existingData,
                                                UnaryOperator<Map<String, Data>> upgrader,
                                                Consumer<String> removeData,
                                                BiConsumer<String, Data> putData) {
        // collect current data
        Map<String, Data> existingMap = new HashMap<>();
        for (ObjectObjectCursor<String, Data> customCursor : existingData) {
            existingMap.put(customCursor.key, customCursor.value);
        }
        // upgrade global custom meta data
        Map<String, Data> upgradedCustoms = upgrader.apply(existingMap);
        if (upgradedCustoms.equals(existingMap) == false) {
            // remove all data first so a plugin can remove custom metadata or templates if needed
            existingMap.keySet().forEach(removeData);
            for (Map.Entry<String, Data> upgradedCustomEntry : upgradedCustoms.entrySet()) {
                putData.accept(upgradedCustomEntry.getKey(), upgradedCustomEntry.getValue());
            }
            return true;
        }
        return false;
    }

    /**
     * Checks that the index can be upgraded to the current version of the master node.
     *
     * <p>
     * If the index does not need upgrade it returns the index metadata unchanged, otherwise it returns a modified index metadata. If index
     * cannot be updated the method throws an exception.
     */
    public IndexMetadata upgradeIndexMetadata(IndexMetadata indexMetadata,
                                              @Nullable IndexTemplateMetadata indexTemplateMetadata,
                                              Version minimumIndexCompatibilityVersion,
                                              @Nullable UserDefinedFunctionsMetadata userDefinedFunctionsMetadata) {
        // Throws an exception if there are too-old segments:
        if (isUpgraded(indexMetadata)) {
            return indexMetadata;
        }
        if (userDefinedFunctionsMetadata != null) {
            userDefinedFunctionService.updateImplementations(userDefinedFunctionsMetadata.functionsMetadata());
        }
        checkSupportedVersion(indexMetadata, minimumIndexCompatibilityVersion);
        IndexMetadata newMetadata = indexMetadata;
        // we have to run this first otherwise in we try to create IndexSettings
        // with broken settings and fail in checkMappingsCompatibility
        newMetadata = archiveBrokenIndexSettings(newMetadata);
        newMetadata = indexUpgrader.upgrade(newMetadata, indexTemplateMetadata);
        checkMappingsCompatibility(newMetadata);
        return markAsUpgraded(newMetadata);
    }

    /**
     * If {@link RelationMetadata} doesn't exist for simple or partitioned tables, create them using the already
     * upgraded {@link IndexMetadata} or {@link IndexTemplateMetadata} respectively. If they exist (this is handled in
     * {@link DocTableInfoFactory#create(RelationName, Metadata)}), then override them to set the settings that have
     * been upgraded on @link IndexMetadata} or {@link IndexTemplateMetadata}.
     */
    private Metadata addOrUpgradeRelationMetadata(Metadata metadata) {
        Metadata.Builder newMetadata = Metadata.builder(metadata);

        for (ObjectObjectCursor<String, IndexTemplateMetadata> entry : metadata.templates()) {
            IndexTemplateMetadata indexTemplateMetadata = entry.value;
            RelationName relationName = IndexName.decode(indexTemplateMetadata.name()).toRelationName();
            RelationMetadata.Table table = newMetadata.getRelation(relationName);
            DocTableInfo docTable = tableFactory.create(indexTemplateMetadata, metadata);
            Version versionCreated = docTable.versionCreated();

            // Versions up to 5.9.7 had a bug where ALTER TABLE on a partitioned table could change the `versionCreated` property on the template.
            // See https://github.com/crate/crate/pull/17178
            // To mitigate the impact, this looks through partitions and takes their lowest version:
            Index[] concreteIndices = IndexNameExpressionResolver.concreteIndices(
                metadata,
                IndicesOptions.LENIENT_EXPAND_OPEN,
                PartitionName.templatePrefix(relationName.schema(), relationName.name())
            );
            for (Index index : concreteIndices) {
                IndexMetadata indexMetadata = metadata.index(index);
                if (indexMetadata != null) {
                    versionCreated = Version.min(versionCreated, indexMetadata.getCreationVersion());
                }
            }
            if (versionCreated.onOrAfter(DocTableInfo.COLUMN_OID_VERSION)) {
                // Due to https://github.com/crate/crate/pull/17178 the created version in the template
                // can be wrong; inferring the correct version from partitions can also fail if old partitions
                // are deleted.
                // This is another safety mechanism. If a column doesn't have OIDs the table must be from < 5.5.0
                if (docTable.rootColumns().stream().anyMatch(ref -> ref.oid() == COLUMN_OID_UNASSIGNED)) {
                    versionCreated = Version.V_5_4_0;
                }
            }

            // versionCreated could be missing from the template settings and "calculated" afterwards
            // in DocTableInfo, so put it back to RelationMetadata#parameters
            Settings settings = Settings.builder()
                // If RelationMetadata exist in the cluster state, make sure to override them with
                // the upgraded settings which currently takes place on IndexTemplateMetadata
                .put(indexTemplateMetadata.settings())
                .put(SETTING_VERSION_CREATED, versionCreated)
                .put(SETTING_VERSION_UPGRADED, Version.CURRENT)
                .build();
            newMetadata.setTable(
                docTable.versionCreated().before(DocTableInfo.COLUMN_OID_VERSION)
                    ? NO_OID_COLUMN_OID_SUPPLIER
                    : newMetadata.columnOidSupplier(),
                relationName,
                docTable.allReferences(),
                settings,
                docTable.clusteredBy(),
                docTable.columnPolicy(),
                docTable.pkConstraintName(),
                docTable.checkConstraints()
                    .stream().collect(Collectors.toMap(CheckConstraint::name, CheckConstraint::expressionStr)),
                docTable.primaryKey(),
                docTable.partitionedBy(),
                docTable.isClosed() ? IndexMetadata.State.CLOSE : IndexMetadata.State.OPEN,
                List.of(),
                table != null ? table.tableVersion() + 1 : docTable.tableVersion()
            );
            // Remove the template, it's not needed anymore
            newMetadata.removeTemplate(indexTemplateMetadata.name());
        }

        for (IndexMetadata indexMetadata : metadata) {
            indexMetadata = upgradeIndexMetadata(indexMetadata, null, Version.V_5_0_0, null);
            DocTableInfo docTable = tableFactory.create(indexMetadata);
            String indexName = indexMetadata.getIndex().getName();
            IndexParts indexParts = IndexName.decode(indexName);
            RelationName relationName = indexParts.toRelationName();
            RelationMetadata relation = newMetadata.getRelation(relationName);
            LongSupplier columnOidSupplier = docTable.versionCreated().before(DocTableInfo.COLUMN_OID_VERSION)
                ? NO_OID_COLUMN_OID_SUPPLIER
                : newMetadata.columnOidSupplier();
            if (relation == null) {
                newMetadata.setTable(
                    columnOidSupplier,
                    relationName,
                    docTable.allReferences(),
                    // If RelationMetadata exist in the cluster state, make sure to override them with
                    // the upgraded settings which currently takes place on IndexMetadata
                    indexMetadata.getSettings(),
                    docTable.clusteredBy(),
                    docTable.columnPolicy(),
                    docTable.pkConstraintName(),
                    docTable.checkConstraints()
                        .stream().collect(Collectors.toMap(CheckConstraint::name, CheckConstraint::expressionStr)),
                    docTable.primaryKey(),
                    docTable.partitionedBy(),
                    indexMetadata.getState(),
                    List.of(indexMetadata.getIndexUUID()),
                    docTable.tableVersion()
                );
            } else if (relation instanceof RelationMetadata.Table table) {
                if (table.indexUUIDs().contains(indexMetadata.getIndexUUID())) {
                    // already added
                    continue;
                }
                if (indexParts.isPartitioned()) {
                    indexMetadata = IndexMetadata.builder(indexMetadata)
                        .partitionValues(PartitionName.decodeIdent(indexParts.partitionIdent()))
                        .build();
                    newMetadata.put(indexMetadata, false);
                }
                newMetadata.addIndexUUIDs(table, List.of(indexMetadata.getIndexUUID()));
            }

        }

        return newMetadata.build();
    }

    private Map<String, IndexTemplateMetadata> upgradeTemplates(Map<String, IndexTemplateMetadata> templates) {
        return templateUpgrader.upgrade(templates);
    }


    /**
     * Checks if the index was already opened by this version of CrateDB and doesn't require any additional checks.
     */
    private boolean isUpgraded(IndexMetadata indexMetadata) {
        return indexMetadata.getCreationVersion().onOrAfter(Version.CURRENT)
            || indexMetadata.getUpgradedVersion().onOrAfter(Version.CURRENT);
    }

    /**
     * Elasticsearch v6.0 no longer supports indices created pre v5.0. All indices
     * that were created before Elasticsearch v5.0 should be re-indexed in Elasticsearch 5.x
     * before they can be opened by this version of elasticsearch.
     */
    private void checkSupportedVersion(IndexMetadata indexMetadata, Version minimumIndexCompatibilityVersion) {
        if (indexMetadata.getState() == IndexMetadata.State.OPEN
            && isSupportedVersion(indexMetadata, minimumIndexCompatibilityVersion) == false) {
            throw new IllegalStateException("The index [" + indexMetadata.getIndex() + "] was created with version ["
                + indexMetadata.getCreationVersion() + "] but the minimum compatible version is ["
                + minimumIndexCompatibilityVersion + "]."
                + "It should be re-indexed in the previous major version of CrateDB before upgrading to " + Version.CURRENT + ".");
        }
    }

    /*
     * Returns true if this index can be supported by the current version of elasticsearch
     */
    private static boolean isSupportedVersion(IndexMetadata indexMetadata, Version minimumIndexCompatibilityVersion) {
        return indexMetadata.getCreationVersion().onOrAfter(minimumIndexCompatibilityVersion);
    }

    /**
     * Checks the mappings for compatibility with the current version
     */
    @VisibleForTesting
    void checkMappingsCompatibility(IndexMetadata indexMetadata) {
        try {
            tableFactory.create(indexMetadata);

            // We cannot instantiate real analysis server or similarity service at this point because the node
            // might not have been started yet. However, we don't really need real analyzers or similarities at
            // this stage - so we can fake it using constant maps accepting every key.
            // This is ok because all used similarities and analyzers for this index were known before the upgrade.
            // Missing analyzers and similarities plugin will still trigger the appropriate error during the
            // actual upgrade.

        } catch (Exception ex) {
            // Wrap the inner exception so we have the index name in the exception message
            throw new IllegalStateException("unable to upgrade the mappings for the index [" + indexMetadata.getIndex() + "]", ex);
        }
    }

    /**
     * Marks index as upgraded so we don't have to test it again
     */
    private IndexMetadata markAsUpgraded(IndexMetadata indexMetadata) {
        Settings settings = Settings.builder().put(indexMetadata.getSettings()).put(SETTING_VERSION_UPGRADED, Version.CURRENT).build();
        return IndexMetadata.builder(indexMetadata).settings(settings).build();
    }

    private IndexMetadata archiveBrokenIndexSettings(IndexMetadata indexMetadata) {
        final Settings settings = indexMetadata.getSettings();
        final Settings upgrade = indexScopedSettings.archiveUnknownOrInvalidSettings(
            settings,
            e -> LOGGER.warn("{} ignoring unknown index setting: [{}] with value [{}]; archiving", indexMetadata.getIndex(), e.getKey(), e.getValue()),
            (e, ex) -> LOGGER.warn(() -> new ParameterizedMessage("{} ignoring invalid index setting: [{}] with value [{}]; archiving", indexMetadata.getIndex(), e.getKey(), e.getValue()), ex));
        if (upgrade != settings) {
            return IndexMetadata.builder(indexMetadata).settings(upgrade).build();
        } else {
            return indexMetadata;
        }
    }
}
