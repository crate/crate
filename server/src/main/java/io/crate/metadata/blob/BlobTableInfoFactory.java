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

package io.crate.metadata.blob;

import java.nio.file.Path;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.jetbrains.annotations.Nullable;

import io.crate.blob.v2.BlobIndex;
import io.crate.blob.v2.BlobIndicesService;
import io.crate.exceptions.RelationUnknown;
import io.crate.metadata.RelationName;
import io.crate.metadata.settings.NumberOfReplicas;

/**
 * Similar to {@link io.crate.metadata.doc.DocTableInfoFactory} this is a factory to create BlobTableInfos'
 *
 * The reason there is no shared interface with generics is that guice cannot bind different implementations based
 * on the generic
 */
public class BlobTableInfoFactory {

    private final Path[] dataFiles;
    private final Path globalBlobPath;

    public BlobTableInfoFactory(Settings settings, Environment environment) {
        this.dataFiles = environment.dataFiles();
        this.globalBlobPath = BlobIndicesService.getGlobalBlobPath(settings);
    }

    @Nullable
    private IndexMetadata resolveIndexMetadata(String tableName, Metadata metadata) {
        String indexName = BlobIndex.fullIndexName(tableName);
        Index index;
        try {
            index = IndexNameExpressionResolver.concreteIndices(metadata, IndicesOptions.STRICT_EXPAND_OPEN, indexName)[0];
        } catch (IndexNotFoundException ex) {
            // Fallback to 6.0+
            return null;
        }
        return metadata.index(index);
    }

    public BlobTableInfo create(RelationName ident, ClusterState clusterState) {

        // Blob tables can be read from pre-6.0 state, try to resolve it in the old way
        // TODO: Remove BWC code on a version that won't read persisted state from <6.0.0
        IndexMetadata indexMetadata = resolveIndexMetadata(ident.name(), clusterState.metadata());

        if (indexMetadata == null) {
            // Read blob table persisted in version 6.0+
            RelationMetadata.BlobTable blobTable = clusterState.metadata().getRelation(ident);
            if (blobTable == null) {

                throw new RelationUnknown(ident);
            }
            indexMetadata = clusterState.metadata().indexByUUID(blobTable.indexUUID());
            if (indexMetadata == null) {
                throw new RelationUnknown(ident);
            }
        }
        Settings settings = indexMetadata.getSettings();
        return new BlobTableInfo(
            ident,
            indexMetadata.getIndex().getName(),
            indexMetadata.getNumberOfShards(),
            NumberOfReplicas.getVirtualValue(settings),
            settings,
            blobsPath(settings),
            IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(settings),
            settings.getAsVersion(IndexMetadata.SETTING_VERSION_UPGRADED, null),
            indexMetadata.getState() == IndexMetadata.State.CLOSE);
    }

    private String blobsPath(Settings indexMetadataSettings) {
        String blobsPath;
        String blobsPathStr = BlobIndicesService.SETTING_INDEX_BLOBS_PATH.get(indexMetadataSettings);
        if (Strings.hasLength(blobsPathStr)) {
            blobsPath = blobsPathStr;
        } else {
            Path path = globalBlobPath;
            if (path != null) {
                blobsPath = path.toString();
            } else {
                // TODO: should we set this to null because there is no special blobPath?
                blobsPath = dataFiles[0].toString();
            }
        }
        return blobsPath;
    }
}
