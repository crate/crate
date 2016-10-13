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
import io.crate.blob.BlobEnvironment;
import io.crate.blob.v2.BlobIndices;
import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.TableIdent;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexNotFoundException;

import java.io.File;
import java.nio.file.Path;

class BlobTableInfoBuilder {

    private final TableIdent ident;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final BlobEnvironment blobEnvironment;
    private final Environment environment;
    private final ClusterState state;
    private final MetaData metaData;
    private String[] concreteIndices;

    BlobTableInfoBuilder(TableIdent ident,
                         ClusterService clusterService,
                         IndexNameExpressionResolver indexNameExpressionResolver,
                         BlobEnvironment blobEnvironment,
                         Environment environment) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.blobEnvironment = blobEnvironment;
        this.environment = environment;
        this.state = clusterService.state();
        this.metaData = state.metaData();
        this.ident = ident;
    }

    private IndexMetaData resolveIndexMetaData() {
        String index = BlobIndices.fullIndexName(ident.name());
        try {
            concreteIndices = indexNameExpressionResolver.concreteIndices(
                state, IndicesOptions.strictExpandOpen(), index);
        } catch (IndexNotFoundException ex) {
            throw new TableUnknownException(index, ex);
        }
        return metaData.index(concreteIndices[0]);
    }

    public BlobTableInfo build() {
        IndexMetaData indexMetaData = resolveIndexMetaData();
        return new BlobTableInfo(
            ident,
            concreteIndices[0],
            clusterService,
            indexMetaData.getNumberOfShards(),
            NumberOfReplicas.fromSettings(indexMetaData.getSettings()),
            TableParameterInfo.tableParametersFromIndexMetaData(indexMetaData),
            blobsPath(indexMetaData.getSettings()));
    }

    private BytesRef blobsPath(Settings indexMetaDataSettings) {
        BytesRef blobsPath;
        String blobsPathStr = indexMetaDataSettings.get(BlobIndices.SETTING_INDEX_BLOBS_PATH);
        if (blobsPathStr != null) {
            blobsPath = new BytesRef(blobsPathStr);
        } else {
            File path = blobEnvironment.blobsPath();
            if (path != null) {
                blobsPath = new BytesRef(path.getPath());
            } else {
                Path[] dataFiles = environment.dataFiles();
                blobsPath = new BytesRef(dataFiles[0].toFile().getAbsolutePath());
            }
        }
        return blobsPath;
    }
}
