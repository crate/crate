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

package org.elasticsearch.repositories.s3;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;

import static org.elasticsearch.repositories.s3.S3RepositorySettings.ACCESS_KEY_SETTING;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.BASE_PATH_SETTING;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.BUCKET_SETTING;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.BUFFER_SIZE_SETTING;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.CANNED_ACL_SETTING;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.CHUNK_SIZE_SETTING;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.ENDPOINT_SETTING;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.MAX_RETRIES_SETTING;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.PROTOCOL_SETTING;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.READONLY_SETTING;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.SECRET_KEY_SETTING;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.SERVER_SIDE_ENCRYPTION_SETTING;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.STORAGE_CLASS_SETTING;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.USE_THROTTLE_RETRIES_SETTING;

/**
 * Shared file system implementation of the BlobStoreRepository
 * <p>
 * Shared file system repository supports the following settings
 * <dl>
 * <dt>{@code bucket}</dt><dd>S3 bucket</dd>
 * <dt>{@code base_path}</dt><dd>Specifies the path within bucket to repository data. Defaults to root directory.</dd>
 * <dt>{@code concurrent_streams}</dt><dd>Number of concurrent read/write stream (per repository on each node). Defaults to 5.</dd>
 * <dt>{@code chunk_size}</dt>
 * <dd>Large file can be divided into chunks. This parameter specifies the chunk size. Defaults to not chucked.</dd>
 * <dt>{@code compress}</dt><dd>If set to true metadata files will be stored compressed. Defaults to false.</dd>
 * </dl>
 */
public class S3Repository extends BlobStoreRepository {

    private static final Logger LOGGER = LogManager.getLogger(S3Repository.class);

    static final String TYPE = "s3";

    public static List<Setting<?>> mandatorySettings() {
        return List.of(ACCESS_KEY_SETTING, SECRET_KEY_SETTING);
    }

    public static List<Setting<?>> optionalSettings() {
        return List.of(BASE_PATH_SETTING,
                       BUCKET_SETTING,
                       BUFFER_SIZE_SETTING,
                       CANNED_ACL_SETTING,
                       CHUNK_SIZE_SETTING,
                       COMPRESS_SETTING,
                       SERVER_SIDE_ENCRYPTION_SETTING,
                       // client specific settings
                       ENDPOINT_SETTING,
                       PROTOCOL_SETTING,
                       MAX_RETRIES_SETTING,
                       USE_THROTTLE_RETRIES_SETTING,
                       READONLY_SETTING);
    }

    private final S3Service service;

    private final String bucket;

    private final ByteSizeValue bufferSize;

    private final ByteSizeValue chunkSize;

    private final BlobPath basePath;

    private final boolean serverSideEncryption;

    private final String storageClass;

    private final String cannedACL;

    /**
     * Constructs an s3 backed repository
     */
    S3Repository(final RepositoryMetaData metadata,
                 final Settings settings,
                 final NamedXContentRegistry namedXContentRegistry,
                 final S3Service service,
                 final ThreadPool threadPool) {
        super(metadata, settings, namedXContentRegistry, threadPool);
        this.service = service;

        // Parse and validate the user's S3 Storage Class setting
        this.bucket = BUCKET_SETTING.get(metadata.settings());
        if (bucket == null) {
            throw new RepositoryException(metadata.name(), "No bucket defined for s3 repository");
        }

        this.bufferSize = BUFFER_SIZE_SETTING.get(metadata.settings());
        this.chunkSize = CHUNK_SIZE_SETTING.get(metadata.settings());

        // We make sure that chunkSize is bigger or equal than/to bufferSize
        if (this.chunkSize.getBytes() < bufferSize.getBytes()) {
            throw new RepositoryException(metadata.name(), CHUNK_SIZE_SETTING.getKey() + " (" + this.chunkSize +
                ") can't be lower than " + BUFFER_SIZE_SETTING.getKey() + " (" + bufferSize + ").");
        }

        final String basePath = BASE_PATH_SETTING.get(metadata.settings());
        if (Strings.hasLength(basePath)) {
            this.basePath = new BlobPath().add(basePath);
        } else {
            this.basePath = BlobPath.cleanPath();
        }

        this.serverSideEncryption = SERVER_SIDE_ENCRYPTION_SETTING.get(metadata.settings());

        this.storageClass = STORAGE_CLASS_SETTING.get(metadata.settings());
        this.cannedACL = CANNED_ACL_SETTING.get(metadata.settings());

        LOGGER.debug(
                "using bucket [{}], chunk_size [{}], server_side_encryption [{}], buffer_size [{}], cannedACL [{}], storageClass [{}]",
                bucket,
                chunkSize,
                serverSideEncryption,
                bufferSize,
                cannedACL,
                storageClass);
    }

    @Override
    protected S3BlobStore createBlobStore() {
        return new S3BlobStore(service, bucket, serverSideEncryption, bufferSize, cannedACL, storageClass, metadata);
    }

    // only use for testing
    @Override
    public BlobStore getBlobStore() {
        return super.getBlobStore();
    }

    @Override
    public BlobPath basePath() {
        return basePath;
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }

    @Override
    protected void doClose() {
        super.doClose();
    }
}
