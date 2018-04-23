/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.external;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.google.common.annotations.VisibleForTesting;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.net.URI;

@NotThreadSafe
public class S3ClientHelper {

    // prefix for s3 client settings
    private static final String PREFIX = "s3.client.";

    // Duplicating settings from S3ClientSettings to avoid adding compile dependency to 'es-repository-s3'.
    static final Setting.AffixSetting<SecureString> ACCESS_KEY_SETTING = Setting.affixKeySetting(PREFIX, "access_key",
        key -> SecureSetting.secureString(key, null));
    static final Setting.AffixSetting<SecureString> SECRET_KEY_SETTING = Setting.affixKeySetting(PREFIX, "secret_key",
        key -> SecureSetting.secureString(key, null));

    static final String ACCESS_KEY_SETTING_NAME = PREFIX + "default.access_key";
    static final String SECRET_KEY_SETTING_NAME = PREFIX + "default.secret_key";

    private final Settings settings;
    private final boolean useDefaultRegion;

    private static final ClientConfiguration CLIENT_CONFIGURATION = new ClientConfiguration().withProtocol(Protocol.HTTPS);

    static {
        CLIENT_CONFIGURATION.setRetryPolicy(PredefinedRetryPolicies.getDefaultRetryPolicyWithCustomMaxRetries(5));
        CLIENT_CONFIGURATION.setUseTcpKeepAlive(true);
    }

    private static final String INVALID_URI_MSG = "Invalid URI. Please make sure that given URI is encoded properly.";

    private final IntObjectMap<AmazonS3> clientMap = new IntObjectHashMap<>(1);

    private S3ClientHelper(Settings settings, boolean useDefaultRegion) {
        this.settings = settings;
        this.useDefaultRegion = useDefaultRegion;
    }

    public S3ClientHelper(Settings settings) {
        this(settings, false);
    }

    @VisibleForTesting
    static S3ClientHelper withDefaultRegion(Settings settings) {
        return new S3ClientHelper(settings, true);
    }

    protected AmazonS3 initClient(String accessKey, String secretKey) throws IOException {
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        if (useDefaultRegion) {
            builder.setRegion("eu-west-1");
        }

        if (!accessKey.isEmpty() && !secretKey.isEmpty()) {
            builder.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                    .withClientConfiguration(CLIENT_CONFIGURATION);
        }
        return builder.build();
    }

    public AmazonS3 client(URI uri) throws IOException {
        if (uri.getHost() == null) {
            throw new IllegalArgumentException(INVALID_URI_MSG);
        }
        // UserInfo should not be part of the URI as the AWS credentials should reside in the keystore
        if (uri.getUserInfo() != null) {
            if (!uri.getUserInfo().isEmpty()) {
                throw new IllegalArgumentException("S3 credentials cannot be part of the URI but instead " +
                                                   "must be stored inside the Crate keystore");
            }
        } else if (uri.toString().contains("@") && uri.toString().contains(":")) {
            // if the URI contains '@' and ':', a UserInfo is in fact given, but could not
            // be parsed properly because the URI is not valid (e.g. not properly encoded).
            throw new IllegalArgumentException(INVALID_URI_MSG);
        }

        return client();
    }

    private AmazonS3 client() throws IOException {
        String accessKey = ACCESS_KEY_SETTING.getConcreteSetting(ACCESS_KEY_SETTING_NAME).get(settings).toString();
        String secretKey = SECRET_KEY_SETTING.getConcreteSetting(SECRET_KEY_SETTING_NAME).get(settings).toString();

        if ((accessKey.isEmpty() && !secretKey.isEmpty()) || (!accessKey.isEmpty() && secretKey.isEmpty())) {
            throw new IllegalArgumentException(
                "Both [" + ACCESS_KEY_SETTING_NAME + "] and [" + SECRET_KEY_SETTING_NAME + "] " +
                "S3 settings for credentials must be stored in the crate.keystore");
        }

        int hash = hash(accessKey, secretKey);
        AmazonS3 client = clientMap.get(hash);
        if (client == null) {
            client = initClient(accessKey, secretKey);
            clientMap.put(hash, client);
        }
        return client;
    }

    private static int hash(@Nullable String accessKey, @Nullable String secretKey) {
        return 31 * (accessKey == null ? 1 : accessKey.hashCode()) + (secretKey == null ? 1 : secretKey.hashCode());
    }
}
