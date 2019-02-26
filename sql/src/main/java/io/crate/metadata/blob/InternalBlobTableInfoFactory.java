/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.analyze.NumberOfReplicas;
import io.crate.analyze.TableParameterInfo;
import io.crate.blob.v2.BlobIndex;
import io.crate.blob.v2.BlobIndicesService;
import io.crate.exceptions.RelationUnknown;
import io.crate.metadata.RelationName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.crate.es.action.support.IndicesOptions;
import io.crate.es.cluster.ClusterState;
import io.crate.es.cluster.metadata.IndexMetaData;
import io.crate.es.cluster.metadata.IndexNameExpressionResolver;
import io.crate.es.common.Strings;
import io.crate.es.common.inject.Inject;
import io.crate.es.common.settings.Settings;
import io.crate.es.env.Environment;
import io.crate.es.index.Index;
import io.crate.es.index.IndexNotFoundException;

import java.nio.file.Path;

public class InternalBlobTableInfoFactory implements BlobTableInfoFactory {

    private static final Logger LOGGER = LogManager.getLogger(InternalBlobTableInfoFactory.class);
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Environment environment;
    private final Path globalBlobPath;

    @Inject
    public InternalBlobTableInfoFactory(Settings settings,
                                        IndexNameExpressionResolver indexNameExpressionResolver,
                                        Environment environment) {
        this.globalBlobPath = BlobIndicesService.getGlobalBlobPath(settings);
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.environment = environment;
    }

    private IndexMetaData resolveIndexMetaData(String tableName, ClusterState state) {
        String indexName = BlobIndex.fullIndexName(tableName);
        Index index;
        try {
            index = indexNameExpressionResolver.concreteIndices(state, IndicesOptions.strictExpandOpen(), indexName)[0];
        } catch (IndexNotFoundException ex) {
            throw new RelationUnknown(indexName, ex);
        }
        return state.metaData().index(index);
    }

    @Override
    public BlobTableInfo create(RelationName ident, ClusterState clusterState) {
        IndexMetaData indexMetaData = resolveIndexMetaData(ident.name(), clusterState);
        Settings settings = indexMetaData.getSettings();
        return new BlobTableInfo(
            ident,
            indexMetaData.getIndex().getName(),
            indexMetaData.getNumberOfShards(),
            NumberOfReplicas.fromSettings(settings),
            TableParameterInfo.tableParametersFromIndexMetaData(indexMetaData),
            blobsPath(settings),
            IndexMetaData.SETTING_INDEX_VERSION_CREATED.get(settings),
            settings.getAsVersion(IndexMetaData.SETTING_VERSION_UPGRADED, null),
            indexMetaData.getState() == IndexMetaData.State.CLOSE);
    }

    private String blobsPath(Settings indexMetaDataSettings) {
        String blobsPath;
        String blobsPathStr = BlobIndicesService.SETTING_INDEX_BLOBS_PATH.get(indexMetaDataSettings);
        if (!Strings.isNullOrEmpty(blobsPathStr)) {
            blobsPath = blobsPathStr;
        } else {
            Path path = globalBlobPath;
            if (path != null) {
                blobsPath = path.toString();
            } else {
                // TODO: should we set this to null because there is no special blobPath?
                Path[] dataFiles = environment.dataFiles();
                blobsPath = dataFiles[0].toString();
            }
        }
        return blobsPath;
    }
}
