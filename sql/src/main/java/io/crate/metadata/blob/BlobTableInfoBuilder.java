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

import io.crate.blob.BlobEnvironment;
import io.crate.blob.v2.BlobIndices;
import io.crate.exceptions.TableUnknownException;
import io.crate.exceptions.UnhandledServerException;
import io.crate.metadata.Functions;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocIndexMetaData;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexNotFoundException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public class BlobTableInfoBuilder {

    private final TableIdent ident;
    private final MetaData metaData;
    private final ClusterService clusterService;
    private final BlobEnvironment blobEnvironment;
    private final Environment environment;
    private final Functions functions;
    private String[] concreteIndices;

    public BlobTableInfoBuilder(TableIdent ident,
                                ClusterService clusterService,
                                BlobEnvironment blobEnvironment,
                                Environment environment,
                                Functions functions) {
        this.clusterService = clusterService;
        this.blobEnvironment = blobEnvironment;
        this.environment = environment;
        this.functions = functions;
        this.metaData = clusterService.state().metaData();
        this.ident = ident;
    }

    public DocIndexMetaData docIndexMetaData() {
        DocIndexMetaData docIndexMetaData;
        String index = BlobIndices.fullIndexName(ident.name());
        try {
            // TODO: FIX ME! concreteIndices is now on IndexNameExpressionResolver
            concreteIndices = null; //metaData.concreteIndices(IndicesOptions.strictExpandOpen(), index);
        } catch (IndexNotFoundException ex) {
            throw new TableUnknownException(index, ex);
        }
        docIndexMetaData = buildDocIndexMetaData(concreteIndices[0]);
        return docIndexMetaData;
    }

    private DocIndexMetaData buildDocIndexMetaData(String index) {
        DocIndexMetaData docIndexMetaData;
        try {
            docIndexMetaData = new DocIndexMetaData(functions, metaData.index(index), ident);
        } catch (IOException e) {
            throw new UnhandledServerException("Unable to build DocIndexMetaData", e);
        }
        return docIndexMetaData.build();
    }

    public BlobTableInfo build() {
        DocIndexMetaData md = docIndexMetaData();
        return new BlobTableInfo(
                ident,
                concreteIndices[0],
                clusterService,
                md.numberOfShards(),
                md.numberOfReplicas(),
                md.tableParameters(),
                blobsPath(md));
    }

    private BytesRef blobsPath(DocIndexMetaData md) {
        BytesRef blobsPath;
        String blobsPathStr = metaData.index(md.concreteIndexName())
                .getSettings().get(BlobIndices.SETTING_INDEX_BLOBS_PATH);
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
