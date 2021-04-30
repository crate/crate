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

package io.crate.testing;

import io.crate.exceptions.RelationUnknown;
import io.crate.metadata.RelationName;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.metadata.blob.BlobTableInfoFactory;
import io.crate.metadata.blob.InternalBlobTableInfoFactory;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;

class TestingBlobTableInfoFactory implements BlobTableInfoFactory {

    private final Map<RelationName, BlobTableInfo> tables;
    private final InternalBlobTableInfoFactory internalFactory;

    TestingBlobTableInfoFactory(Map<RelationName, BlobTableInfo> blobTables) {
        this.tables = blobTables;
        this.internalFactory = null;
    }

    TestingBlobTableInfoFactory(Map<RelationName, BlobTableInfo> blobTables,
                                IndexNameExpressionResolver indexNameExpressionResolver,
                                File dataPath) {
        this.tables = blobTables;
        internalFactory = new InternalBlobTableInfoFactory(
            Settings.EMPTY, indexNameExpressionResolver, new Path[]{Path.of(dataPath.toURI())});
    }


    @Override
    public BlobTableInfo create(RelationName ident, ClusterState state) {
        BlobTableInfo blobTableInfo = tables.get(ident);
        if (blobTableInfo == null) {
            if (internalFactory == null) {
                throw new RelationUnknown(ident);
            }
            return internalFactory.create(ident, state);
        }
        return blobTableInfo;
    }
}
