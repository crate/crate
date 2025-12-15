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
import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;
import static org.elasticsearch.cluster.metadata.Metadata.Builder.NO_OID_COLUMN_OID_SUPPLIER;

import java.util.List;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.settings.AbstractScopedSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.jspecify.annotations.Nullable;
import io.crate.common.annotations.VisibleForTesting;

import io.crate.blob.v2.BlobIndex;
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
@SuppressWarnings("deprecation") // suppressed because this class by design deals with migrating from deprecated components to non-deprecated.
public class MetadataUpgradeService {

    private static final Logger LOGGER = LogManager.getLogger(MetadataUpgradeService.class);

    private final IndexScopedSettings indexScopedSettings;
    private final MetadataIndexUpgrader indexUpgrader;
    private final DocTableInfoFactory tableFactory;
    private final UserDefinedFunctionService userDefinedFunctionService;

    public MetadataUpgradeService(NodeContext nodeContext,
                                  IndexScopedSettings indexScopedSettings,
                                  UserDefinedFunctionService userDefinedFunctionService) {
        this.tableFactory = new DocTableInfoFactory(nodeContext);
        this.indexScopedSettings = indexScopedSettings;
        this.indexUpgrader = new MetadataIndexUpgrader();
        this.userDefinedFunctionService = userDefinedFunctionService;
    }

    public Metadata upgradeMetadata(Metadata metadata) {
        final Metadata.Builder newMetadata = Metadata.builder(metadata);

        UserDefinedFunctionsMetadata udfMetadata = metadata.custom(UserDefinedFunctionsMetadata.TYPE);
        if (udfMetadata != null) {
            userDefinedFunctionService.updateImplementations(udfMetadata.functionsMetadata());
        }

        // Templates only exist in Metadata from < 6.1.0
        // If streaming Metadata to nodes < 6.1.0 templates are re-created on demand
        for (var cursor : metadata.templates()) {
            String templateName = cursor.key;
            newMetadata.removeTemplate(templateName);
            if (templateName == IndexTemplateUpgrader.CRATE_DEFAULTS) {
                continue;
            }
            IndexTemplateMetadata template = cursor.value;
            RelationName relationName = IndexName.decode(template.name()).toRelationName();
            RelationMetadata relation = metadata.getRelation(relationName);
            assert relation == null
                : "If there is still a template present there shouldn't be any RelationMetadata";

            DocTableInfo docTable = tableFactory.create(template, metadata);
            Version versionCreated = getFixedVersionCreated(metadata, docTable);

            // versionCreated could be missing from the template settings and "calculated" afterwards
            // in DocTableInfo, so put it back to RelationMetadata#parameters
            Settings settings = Settings.builder()
                // If RelationMetadata exist in the cluster state, make sure to override them with
                // the upgraded settings which currently takes place on IndexTemplateMetadata
                .put(pruneArchived(template.settings()))
                .put(SETTING_VERSION_CREATED, versionCreated)
                .put(SETTING_VERSION_UPGRADED, Version.CURRENT)
                .build();

            newMetadata.setTable(
                versionCreated.before(DocTableInfo.COLUMN_OID_VERSION)
                    ? NO_OID_COLUMN_OID_SUPPLIER
                    : new DocTableInfo.OidSupplier(docTable.maxOid()),
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
                docTable.tableVersion()
            );
        }

        // upgrade index meta data
        for (IndexMetadata indexMetadata : metadata) {
            String indexName = indexMetadata.getIndex().getName();
            IndexMetadata newIndexMetadata = upgradeIndexMetadata(
                indexMetadata,
                IndexName.isPartitioned(indexName)
                    ? newMetadata.getTemplate(PartitionName.templateName(indexName))
                    : null,
                Version.CURRENT.minimumIndexCompatibilityVersion());
            // Remove any existing metadata, registered by it's name, for the index
            newMetadata.remove(indexName);
            newMetadata.put(newIndexMetadata, false);

            String indexUUID = indexMetadata.getIndexUUID();
            RelationMetadata relation = metadata.getRelation(indexUUID);
            DocTableInfo tableInfo = null;
            if (relation == null) {
                IndexParts indexParts = IndexName.decode(indexName);
                RelationName relationName = indexParts.toRelationName();
                relation = newMetadata.getRelation(relationName);
                if (!BlobIndex.isBlobIndex(indexName)) {
                    tableInfo = tableFactory.create(newIndexMetadata);
                }
            }
            if (relation == null) {
                if (tableInfo instanceof DocTableInfo docTable) {
                    LongSupplier columnOidSupplier = docTable.versionCreated().before(DocTableInfo.COLUMN_OID_VERSION)
                        ? NO_OID_COLUMN_OID_SUPPLIER
                        : new DocTableInfo.OidSupplier(docTable.maxOid());
                    newMetadata.setTable(
                        columnOidSupplier,
                        docTable.ident(),
                        docTable.allReferences(),
                        // If RelationMetadata exist in the cluster state, make sure to override them with
                        // the upgraded settings which currently takes place on IndexMetadata
                        newIndexMetadata.getSettings(),
                        docTable.clusteredBy(),
                        docTable.columnPolicy(),
                        docTable.pkConstraintName(),
                        docTable.checkConstraints()
                            .stream().collect(Collectors.toMap(CheckConstraint::name, CheckConstraint::expressionStr)),
                        docTable.primaryKey(),
                        docTable.partitionedBy(),
                        newIndexMetadata.getState(),
                        List.of(newIndexMetadata.getIndexUUID()),
                        docTable.tableVersion()
                    );
                } else if (BlobIndex.isBlobIndex(indexName)) {
                    newMetadata.setBlobTable(
                        RelationName.fromIndexName(indexName),
                        indexUUID,
                        newIndexMetadata.getSettings(),
                        newIndexMetadata.getState()
                    );
                } else {
                    throw new AssertionError("If the relation is missing we need a DocTableInfo instance or it must be a blob index");
                }
            } else if (relation instanceof RelationMetadata.Table table) {
                if (!table.indexUUIDs().contains(indexUUID)) {
                    newMetadata.addIndexUUIDs(table, List.of(indexUUID));
                }
            } else if (relation instanceof RelationMetadata.BlobTable blobTable) {
                assert blobTable.indexUUID().equals(indexUUID)
                    : "If there exists a RelationMetadata.BlobTable entry the incoming IndexMetadata indexUUID must match";
            }
        }

        return newMetadata.build();
    }

    private static Settings pruneArchived(Settings settings) {
        return IndexScopedSettings.DEFAULT_SCOPED_SETTINGS.archiveUnknownOrInvalidSettings(
            settings,
            _ -> {},
            (_, _) -> {}
        ).filter(k -> !k.startsWith(AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX));
    }


    /// Return a "fixed" version
    ///
    /// See https://github.com/crate/crate/pull/17178
    /// In versions up to 5.9.7 ALTER TABLE on a partitioned table could change the
    /// `versionCreated` property on the template.
    ///
    private static Version getFixedVersionCreated(Metadata metadata, DocTableInfo docTable) {
        Version versionCreated = docTable.versionCreated();

        // Look through partitions and takes their lowest version
        RelationName relationName = docTable.ident();
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

        // If partitions were deleted the above logic can still yield wrong versions.
        // This does another "correction" by checking oid assignment
        // If a column doesn't have OIDs the table must be from < 5.5.0
        if (versionCreated.onOrAfter(DocTableInfo.COLUMN_OID_VERSION)) {
            if (docTable.rootColumns().stream().anyMatch(ref -> ref.oid() == COLUMN_OID_UNASSIGNED)) {
                versionCreated = Version.V_5_4_0;
            }
        }
        return versionCreated;
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
                                              Version minimumIndexCompatibilityVersion) {
        if (isUpgraded(indexMetadata)) {
            return indexMetadata;
        }
        // Throws an exception if there are too-old segments:
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
