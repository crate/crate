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

package org.elasticsearch.repositories.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import io.crate.common.unit.TimeValue;
import io.crate.types.DataTypes;

import org.elasticsearch.monitor.jvm.JvmInfo;

import java.util.Locale;

class S3RepositorySettings {

    static final Setting<Boolean> READONLY_SETTING = Setting.boolSetting("readonly", false);

    /**
     * The access key to authenticate with s3.
     */
    static final Setting<SecureString> ACCESS_KEY_SETTING = Setting.maskedString("access_key");

    /**
     * The secret key to authenticate with s3.
     */
    static final Setting<SecureString> SECRET_KEY_SETTING = Setting.maskedString("secret_key");

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
    static final Setting<Boolean> SERVER_SIDE_ENCRYPTION_SETTING =
        Setting.boolSetting("server_side_encryption", false);

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
    private static final ByteSizeValue MAX_PART_SIZE_USING_MULTIPART = MAX_FILE_SIZE;

    /**
     * Maximum size of files that can be uploaded using the Multipart Upload API.
     */
    static final ByteSizeValue MAX_FILE_SIZE_USING_MULTIPART = new ByteSizeValue(5, ByteSizeUnit.TB);

    /**
     * Minimum threshold below which the chunk is uploaded using a single request. Beyond this threshold,
     * the S3 repository will use the AWS Multipart Upload API to split the chunk into several parts,
     * each of buffer_size length, and to upload each part in its own request. Note that setting a
     * buffer size lower than 5mb is not allowed since it will prevents the use of the Multipart API
     * and may result in upload errors. Defaults to the minimum between 100MB and 5% of the heap size.
     */
    static final Setting<ByteSizeValue> BUFFER_SIZE_SETTING = Setting.byteSizeSetting(
        "buffer_size",
        DEFAULT_BUFFER_SIZE,
        MIN_PART_SIZE_USING_MULTIPART,
        MAX_PART_SIZE_USING_MULTIPART);

    /**
     * Big files can be broken down into chunks during snapshotting if needed. Defaults to 1g.
     */
    static final Setting<ByteSizeValue> CHUNK_SIZE_SETTING = Setting.byteSizeSetting(
        "chunk_size",
        new ByteSizeValue(1, ByteSizeUnit.GB),
        new ByteSizeValue(5, ByteSizeUnit.MB),
        new ByteSizeValue(5, ByteSizeUnit.TB));

    /**
     * Sets the S3 storage class type for the backup files. Values may be standard, reduced_redundancy,
     * standard_ia. Defaults to standard.
     */
    static final Setting<String> STORAGE_CLASS_SETTING = Setting.simpleString("storage_class");

    /**
     * The S3 repository supports all S3 canned ACLs : private, public-read, public-read-write,
     * authenticated-read, log-delivery-write, bucket-owner-read, bucket-owner-full-control.
     * Defaults to private.
     */
    static final Setting<String> CANNED_ACL_SETTING = Setting.simpleString("canned_acl");

    /**
     * Specifies the path within bucket to repository data. Defaults to root directory.
     */
    static final Setting<String> BASE_PATH_SETTING = Setting.simpleString("base_path");

    /**
     * The secret key (ie password) for connecting to s3.
     */
    static final Setting<SecureString> SESSION_TOKEN_SETTING = Setting.maskedString("session_token");

    /**
     * An override for the s3 endpoint to connect to.
     */
    static final Setting<String> ENDPOINT_SETTING = new Setting<>(
        "endpoint",
        "",
        s -> s.toLowerCase(Locale.ROOT),
        DataTypes.STRING,
        Setting.Property.NodeScope);

    /**
     * The protocol to use to connect to s3.
     */
    static final Setting<Protocol> PROTOCOL_SETTING = new Setting<>(
        "protocol",
        "https",
        s -> Protocol.valueOf(s.toUpperCase(Locale.ROOT)),
        DataTypes.STRING,
        Setting.Property.NodeScope);

    /**
     * The host name of a proxy to connect to s3 through.
     */
    static final Setting<String> PROXY_HOST_SETTING =
        Setting.simpleString("proxy_host", Setting.Property.NodeScope);

    /**
     * The port of a proxy to connect to s3 through.
     */
    static final Setting<Integer> PROXY_PORT_SETTING =
        Setting.intSetting("proxy_port", 80, 0, 1 << 16, Setting.Property.NodeScope);

    /**
     * The username of a proxy to connect to s3 through.
     */
    static final Setting<SecureString> PROXY_USERNAME_SETTING = Setting.maskedString("proxy_username");

    /**
     * The password of a proxy to connect to s3 through.
     */
    static final Setting<SecureString> PROXY_PASSWORD_SETTING = Setting.maskedString("proxy_password");

    /**
     * The socket timeout for connecting to s3.
     */
    static final Setting<TimeValue> READ_TIMEOUT_SETTING = Setting.timeSetting(
        "read_timeout",
        TimeValue.timeValueMillis(ClientConfiguration.DEFAULT_SOCKET_TIMEOUT),
        Setting.Property.NodeScope);

    /**
     * The number of retries to use when an s3 request fails.
     */
    static final Setting<Integer> MAX_RETRIES_SETTING = Setting.intSetting(
        "max_retries",
        ClientConfiguration.DEFAULT_RETRY_POLICY.getMaxErrorRetry(),
        0,
        Setting.Property.NodeScope);

    /**
     * Whether retries should be throttled (ie use backoff).
     */
    static final Setting<Boolean> USE_THROTTLE_RETRIES_SETTING = Setting.boolSetting(
        "use_throttle_retries",
        ClientConfiguration.DEFAULT_THROTTLE_RETRIES,
        Setting.Property.NodeScope);


    /**
     * Whether to use Path Style Access.
     */
    static final Setting<Boolean> USE_PATH_STYLE_ACCESS = Setting.boolSetting(
        "use_path_style_access",
        false);
}
