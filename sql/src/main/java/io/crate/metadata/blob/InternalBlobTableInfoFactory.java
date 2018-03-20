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

import io.crate.Constants;
import io.crate.analyze.NumberOfReplicas;
import io.crate.analyze.TableParameterInfo;
import io.crate.blob.v2.BlobIndex;
import io.crate.blob.v2.BlobIndicesService;
import io.crate.exceptions.RelationUnknown;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocIndexMetaData;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;

import java.nio.file.Path;
import java.util.Map;

public class InternalBlobTableInfoFactory implements BlobTableInfoFactory {

    private static final Logger LOGGER = Loggers.getLogger(InternalBlobTableInfoFactory.class);
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
    public BlobTableInfo create(TableIdent ident, ClusterState clusterState) {
        IndexMetaData indexMetaData = resolveIndexMetaData(ident.name(), clusterState);
        Map<String, Object> mappingMap;
        try {
            mappingMap = indexMetaData.mapping(Constants.DEFAULT_MAPPING_TYPE).getSourceAsMap();
        } catch (ElasticsearchParseException e) {
            LOGGER.error("error extracting blob table info for {}", e, ident.fqn());
            throw new RuntimeException(e);
        }
        return new BlobTableInfo(
            ident,
            indexMetaData.getIndex().getName(),
            indexMetaData.getNumberOfShards(),
            NumberOfReplicas.fromSettings(indexMetaData.getSettings()),
            TableParameterInfo.tableParametersFromIndexMetaData(indexMetaData),
            blobsPath(indexMetaData.getSettings()),
            DocIndexMetaData.getVersionCreated(mappingMap),
            DocIndexMetaData.getVersionUpgraded(mappingMap),
            indexMetaData.getState() == IndexMetaData.State.CLOSE);
    }

    private BytesRef blobsPath(Settings indexMetaDataSettings) {
        BytesRef blobsPath;
        String blobsPathStr = BlobIndicesService.SETTING_INDEX_BLOBS_PATH.get(indexMetaDataSettings);
        if (!Strings.isNullOrEmpty(blobsPathStr)) {
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
