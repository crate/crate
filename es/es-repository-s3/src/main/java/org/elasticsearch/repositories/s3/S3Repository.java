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

import com.amazonaws.auth.BasicAWSCredentials;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.util.function.Function;

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
class S3Repository extends BlobStoreRepository {

    static final String TYPE = "s3";

    /** The access key to authenticate with s3. This setting is insecure because cluster settings are stored in cluster state */
    static final Setting<SecureString> ACCESS_KEY_SETTING = SecureSetting.insecureString("access_key");

    /** The secret key to authenticate with s3. This setting is insecure because cluster settings are stored in cluster state */
    static final Setting<SecureString> SECRET_KEY_SETTING = SecureSetting.insecureString("secret_key");

    /**
     * Default is to use 100MB (S3 defaults) for heaps above 2GB and 5% of
     * the available memory for smaller heaps.
     */
    private static final ByteSizeValue DEFAULT_BUFFER_SIZE = new ByteSizeValue(
        Math.max(
            ByteSizeUnit.MB.toBytes(5), // minimum value
            Math.min(
                ByteSizeUnit.MB.toBytes(100),
                JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() / 20)),
        ByteSizeUnit.BYTES);


    static final Setting<String> BUCKET_SETTING = Setting.simpleString("bucket");

    /**
     * When set to true files are encrypted on server side using AES256 algorithm.
     * Defaults to false.
     */
    static final Setting<Boolean> SERVER_SIDE_ENCRYPTION_SETTING = Setting.boolSetting("server_side_encryption", false);

    /**
     * Maximum size of files that can be uploaded using a single upload request.
     */
    static final ByteSizeValue MAX_FILE_SIZE = new ByteSizeValue(5, ByteSizeUnit.GB);

    /**
     * Minimum size of parts that can be uploaded using the Multipart Upload API.
     * (see http://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html)
     */
    static final ByteSizeValue MIN_PART_SIZE_USING_MULTIPART = new ByteSizeValue(5, ByteSizeUnit.MB);

    /**
     * Maximum size of parts that can be uploaded using the Multipart Upload API.
     * (see http://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html)
     */
    static final ByteSizeValue MAX_PART_SIZE_USING_MULTIPART = MAX_FILE_SIZE;

    /**
     * Maximum size of files that can be uploaded using the Multipart Upload API.
     */
    static final ByteSizeValue MAX_FILE_SIZE_USING_MULTIPART = new ByteSizeValue(5, ByteSizeUnit.TB);

    /**
     * Minimum threshold below which the chunk is uploaded using a single request. Beyond this threshold,
     * the S3 repository will use the AWS Multipart Upload API to split the chunk into several parts, each of buffer_size length, and
     * to upload each part in its own request. Note that setting a buffer size lower than 5mb is not allowed since it will prevents the
     * use of the Multipart API and may result in upload errors. Defaults to the minimum between 100MB and 5% of the heap size.
     */
    static final Setting<ByteSizeValue> BUFFER_SIZE_SETTING =
        Setting.byteSizeSetting("buffer_size", DEFAULT_BUFFER_SIZE, MIN_PART_SIZE_USING_MULTIPART, MAX_PART_SIZE_USING_MULTIPART);

    /**
     * Big files can be broken down into chunks during snapshotting if needed. Defaults to 1g.
     */
    static final Setting<ByteSizeValue> CHUNK_SIZE_SETTING = Setting.byteSizeSetting("chunk_size", new ByteSizeValue(1, ByteSizeUnit.GB),
            new ByteSizeValue(5, ByteSizeUnit.MB), new ByteSizeValue(5, ByteSizeUnit.TB));

    /**
     * When set to true metadata files are stored in compressed format. This setting doesn’t affect index
     * files that are already compressed by default. Defaults to false.
     */
    static final Setting<Boolean> COMPRESS_SETTING = Setting.boolSetting("compress", false);

    /**
     * Sets the S3 storage class type for the backup files. Values may be standard, reduced_redundancy,
     * standard_ia. Defaults to standard.
     */
    static final Setting<String> STORAGE_CLASS_SETTING = Setting.simpleString("storage_class");

    /**
     * The S3 repository supports all S3 canned ACLs : private, public-read, public-read-write,
     * authenticated-read, log-delivery-write, bucket-owner-read, bucket-owner-full-control. Defaults to private.
     */
    static final Setting<String> CANNED_ACL_SETTING = Setting.simpleString("canned_acl");

    static final Setting<String> CLIENT_NAME = new Setting<>("client", "default", Function.identity());

    /**
     * Specifies the path within bucket to repository data. Defaults to root directory.
     */
    static final Setting<String> BASE_PATH_SETTING = Setting.simpleString("base_path");

    private final S3Service service;

    private final String bucket;

    private final ByteSizeValue bufferSize;

    private final ByteSizeValue chunkSize;

    private final boolean compress;

    private final BlobPath basePath;

    private final boolean serverSideEncryption;

    private final String storageClass;

    private final String cannedACL;

    private final String clientName;

    private final AmazonS3Reference reference;

    /**
     * Constructs an s3 backed repository
     */
    S3Repository(final RepositoryMetaData metadata,
                 final Settings settings,
                 final NamedXContentRegistry namedXContentRegistry,
                 final S3Service service) {
        super(metadata, settings, namedXContentRegistry);
        this.service = service;

        // Parse and validate the user's S3 Storage Class setting
        this.bucket = BUCKET_SETTING.get(metadata.settings());
        if (bucket == null) {
            throw new RepositoryException(metadata.name(), "No bucket defined for s3 repository");
        }

        this.bufferSize = BUFFER_SIZE_SETTING.get(metadata.settings());
        this.chunkSize = CHUNK_SIZE_SETTING.get(metadata.settings());
        this.compress = COMPRESS_SETTING.get(metadata.settings());

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

        this.clientName = CLIENT_NAME.get(metadata.settings());

        if (CLIENT_NAME.exists(metadata.settings()) && S3ClientSettings.checkDeprecatedCredentials(metadata.settings())) {
            logger.warn(
                    "ignoring use of named client [{}] for repository [{}] as insecure credentials were specified",
                    clientName,
                    metadata.name());
        }

        if (S3ClientSettings.checkDeprecatedCredentials(metadata.settings())) {
            // provided repository settings
            deprecationLogger.deprecated("Using s3 access/secret key from repository settings. Instead "
                    + "store these in named clients and the elasticsearch keystore for secure settings.");
            final BasicAWSCredentials insecureCredentials = S3ClientSettings.loadDeprecatedCredentials(metadata.settings());
            final S3ClientSettings s3ClientSettings = S3ClientSettings.getClientSettings(metadata, insecureCredentials);
            this.reference = new AmazonS3Reference(service.buildClient(s3ClientSettings));
        } else {
            reference = null;
        }

        logger.debug(
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
        if (reference != null) {
            assert S3ClientSettings.checkDeprecatedCredentials(metadata.settings()) : metadata.name();
            return new S3BlobStore(settings, service, clientName, bucket, serverSideEncryption, bufferSize, cannedACL, storageClass) {
                @Override
                public AmazonS3Reference clientReference() {
                    if (reference.tryIncRef()) {
                        return reference;
                    } else {
                        throw new IllegalStateException("S3 client is closed");
                    }
                }
            };
        } else {
            return new S3BlobStore(settings, service, clientName, bucket, serverSideEncryption, bufferSize, cannedACL, storageClass);
        }
    }

    // only use for testing
    @Override
    protected BlobStore blobStore() {
        return super.blobStore();
    }

    // only use for testing
    @Override
    protected BlobStore getBlobStore() {
        return super.getBlobStore();
    }

    @Override
    protected BlobPath basePath() {
        return basePath;
    }

    @Override
    protected boolean isCompress() {
        return compress;
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }

    @Override
    protected void doClose() {
        if (reference != null) {
            assert S3ClientSettings.checkDeprecatedCredentials(metadata.settings()) : metadata.name();
            reference.decRef();
        }
        super.doClose();
    }

}
