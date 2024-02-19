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

package io.crate.gcs;

import static org.elasticsearch.common.settings.Setting.Property;
import static org.elasticsearch.common.settings.Setting.byteSizeSetting;
import static org.elasticsearch.common.settings.Setting.simpleString;


import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

/**
 * Based on https://github.com/opensearch-project/OpenSearch/blob/main/plugins/repository-gcs/src/main/java/org/opensearch/repositories/gcs/GoogleCloudStorageRepository.java
 */
public class GCSRepository extends BlobStoreRepository {
    // package private for testing
    static final ByteSizeValue MIN_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.BYTES);

    /**
     * Maximum allowed object size in GCS.
     *
     * @see <a href="https://cloud.google.com/storage/quotas#objects">GCS documentation</a> for details.
     */
    static final ByteSizeValue MAX_CHUNK_SIZE = new ByteSizeValue(5, ByteSizeUnit.TB);

    static final Setting<String> BUCKET_SETTING = simpleString("bucket", Property.NodeScope, Property.Dynamic);

    static final Setting<String> BASE_PATH_SETTING = simpleString("base_path", Property.NodeScope, Property.Dynamic);

    static final Setting<ByteSizeValue> CHUNK_SIZE_SETTING = byteSizeSetting(
        "chunk_size",
        MAX_CHUNK_SIZE,
        MIN_CHUNK_SIZE,
        MAX_CHUNK_SIZE,
        Property.NodeScope,
        Property.Dynamic
    );

    private final GCSService service;
    private final ByteSizeValue chunkSize;
    private final String bucket;


    public GCSRepository(
        final RepositoryMetadata metadata,
        final NamedXContentRegistry namedXContentRegistry,
        final ClusterService clusterService,
        final GCSService service,
        final RecoverySettings recoverySettings) {
        super(metadata, namedXContentRegistry, clusterService, recoverySettings, buildBasePath(metadata));
        this.service = service;
        this.chunkSize = CHUNK_SIZE_SETTING.get(metadata.settings());
        this.bucket = BUCKET_SETTING.get(metadata.settings());
    }

    @Override
    protected GCSBlobStore createBlobStore() {
        return new GCSBlobStore(bucket, service, metadata, bufferSize);
    }

    private static BlobPath buildBasePath(RepositoryMetadata metadata) {
        String basePath = BASE_PATH_SETTING.get(metadata.settings());
        if (Strings.hasLength(basePath)) {
            BlobPath path = new BlobPath();
            for (String elem : basePath.split("/")) {
                path = path.add(elem);
            }
            return path;
        } else {
            return BlobPath.cleanPath();
        }
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }
}
