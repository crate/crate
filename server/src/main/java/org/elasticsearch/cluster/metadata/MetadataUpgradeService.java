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

import static org.elasticsearch.cluster.metadata.Metadata.Builder.NO_OID_COLUMN_OID_SUPPLIER;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.expression.udf.UserDefinedFunctionsMetadata;
import io.crate.metadata.IndexName;
import io.crate.metadata.NodeContext;
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
     * Checks that the index can be upgraded to the current version of the master node.
     *
     * <p>
     * If the index does not need upgrade it returns the index metadata unchanged, otherwise it returns a modified index metadata. If index
     * cannot be updated the method throws an exception.
     */
    public IndexMetadata upgradeIndexMetadata(IndexMetadata indexMetadata,
                                              IndexTemplateMetadata indexTemplateMetadata,
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

    public Metadata upgradeRelationMetadata(Metadata metadata) {
        Metadata.Builder newMetadata = Metadata.builder(metadata);
        for (IndexMetadata indexMetadata : metadata) {
            String indexName = indexMetadata.getIndex().getName();
            if (IndexName.isPartitioned(indexName) == false) {
                RelationName relationName = IndexName.decode(indexName).toRelationName();
                DocTableInfo docTable = tableFactory.create(relationName, metadata);
                List<String> indexUUIDs = metadata.getIndices(
                    relationName,
                    List.of(),
                    false,
                    IndexMetadata::getIndexUUID
                );

                newMetadata.setTable(
                    docTable.versionCreated().before(DocTableInfo.COLUMN_OID_VERSION)
                        ? NO_OID_COLUMN_OID_SUPPLIER
                        : newMetadata.columnOidSupplier(),
                    relationName,
                    docTable.references(),
                    indexMetadata.getSettings(),
                    docTable.clusteredBy(),
                    docTable.columnPolicy(),
                    docTable.pkConstraintName(),
                    docTable.checkConstraints()
                        .stream().collect(Collectors.toMap(CheckConstraint::name, CheckConstraint::expressionStr)),
                    docTable.primaryKey(),
                    docTable.partitionedBy(),
                    indexMetadata.getState(),
                    indexUUIDs
                );
            }
        }
        return newMetadata.build();
    }

    public Map<String, IndexTemplateMetadata> upgradeTemplates(Map<String, IndexTemplateMetadata> templates) {
        return templateUpgrader.upgrade(templates);
    }


    /**
     * Checks if the index was already opened by this version of Elasticsearch and doesn't require any additional checks.
     */
    boolean isUpgraded(IndexMetadata indexMetadata) {
        return indexMetadata.getUpgradedVersion().onOrAfter(Version.CURRENT);
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
            tableFactory.validateSchema(indexMetadata);

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
        Settings settings = Settings.builder().put(indexMetadata.getSettings()).put(IndexMetadata.SETTING_VERSION_UPGRADED, Version.CURRENT).build();
        return IndexMetadata.builder(indexMetadata).settings(settings).build();
    }

    IndexMetadata archiveBrokenIndexSettings(IndexMetadata indexMetadata) {
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
