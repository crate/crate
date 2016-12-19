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
import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.TableIdent;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexNotFoundException;

import java.nio.file.Path;

public class InternalBlobTableInfoFactory implements BlobTableInfoFactory {

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
        String index = BlobIndex.fullIndexName(tableName);
        String[] concreteIndices;
        try {
            concreteIndices = indexNameExpressionResolver.concreteIndices(
                state, IndicesOptions.strictExpandOpen(), index);
        } catch (IndexNotFoundException ex) {
            throw new TableUnknownException(index, ex);
        }
        return state.metaData().index(concreteIndices[0]);
    }

    @Override
    public BlobTableInfo create(TableIdent ident, ClusterService clusterService) {
        IndexMetaData indexMetaData = resolveIndexMetaData(ident.name(), clusterService.state());
        return new BlobTableInfo(
            ident,
            indexMetaData.getIndex(),
            clusterService,
            indexMetaData.getNumberOfShards(),
            NumberOfReplicas.fromSettings(indexMetaData.getSettings()),
            TableParameterInfo.tableParametersFromIndexMetaData(indexMetaData),
            blobsPath(indexMetaData.getSettings()));
    }

    private BytesRef blobsPath(Settings indexMetaDataSettings) {
        BytesRef blobsPath;
        String blobsPathStr = indexMetaDataSettings.get(BlobIndicesService.SETTING_INDEX_BLOBS_PATH);
        if (blobsPathStr != null) {
            blobsPath = new BytesRef(blobsPathStr);
        } else {
            Path path = globalBlobPath;
            if (path != null) {
                blobsPath = new BytesRef(path.toString());
            } else {
                // TODO: should we set this to null because there is no special blobPath?
                Path[] dataFiles = environment.dataFiles();
                blobsPath = new BytesRef(dataFiles[0].toString());
            }
        }
        return blobsPath;
    }
}
