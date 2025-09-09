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

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

import io.crate.blob.v2.BlobIndicesService;
import io.crate.exceptions.RelationUnknown;
import io.crate.metadata.RelationName;
import io.crate.metadata.TableInfoFactory;
import io.crate.metadata.settings.NumberOfReplicas;

/**
 * Similar to {@link io.crate.metadata.doc.DocTableInfoFactory} this is a factory to create BlobTableInfos'
 *
 * The reason there is no shared interface with generics is that guice cannot bind different implementations based
 * on the generic
 */
public class BlobTableInfoFactory implements TableInfoFactory<BlobTableInfo> {

    private final Path[] dataFiles;
    private final Path globalBlobPath;

    public BlobTableInfoFactory(Environment environment) {
        this.dataFiles = environment.dataFiles();
        this.globalBlobPath = BlobIndicesService.getGlobalBlobPath(environment.settings());
    }

    @Override
    public BlobTableInfo create(RelationName ident, Metadata metadata) {
        RelationMetadata.BlobTable blobTable = metadata.getRelation(ident);
        if (blobTable == null) {
            throw new RelationUnknown(ident);
        }
        String indexUUID = blobTable.indexUUID();
        IndexMetadata indexMetadata = metadata.index(indexUUID);
        if (indexMetadata == null) {
            throw new RelationUnknown(ident);
        }
        Settings tableSettings = blobTable.settings();
        Settings indexSettings = indexMetadata.getSettings();
        return new BlobTableInfo(
            ident,
            indexMetadata.getNumberOfShards(),
            NumberOfReplicas.getVirtualValue(indexSettings),
            tableSettings,
            blobsPath(tableSettings),
            indexMetadata.getCreationVersion(),
            indexSettings.getAsVersion(IndexMetadata.SETTING_VERSION_UPGRADED, null),
            blobTable.state() == State.CLOSE
        );
    }

    private String blobsPath(Settings indexMetadataSettings) {
        String blobsPathStr = BlobIndicesService.SETTING_INDEX_BLOBS_PATH.get(indexMetadataSettings);
        if (Strings.hasLength(blobsPathStr)) {
            return blobsPathStr;
        }
        Path path = globalBlobPath;
        if (path == null) {
            return dataFiles[0].toString();
        } else {
            return path.toString();
        }
    }
}
