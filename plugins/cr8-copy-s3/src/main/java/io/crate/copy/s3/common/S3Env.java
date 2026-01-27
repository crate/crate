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

package io.crate.copy.s3.common;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.opendal.ServiceConfig.S3;
import org.elasticsearch.common.settings.Settings;
import org.jspecify.annotations.Nullable;

public class S3Env {

    private static final Logger LOGGER = LogManager.getLogger(S3Env.class);
    private static final String DEFAULT_REGION = "us-east-1";
    private static final Pattern AWS_ENDPOINT = Pattern.compile("https://s3\\.(.*)\\.amazonaws\\.com");

    public static S3 getServiceConfig(S3URI s3uri, Settings withClause) {
        String protocol = S3Protocol.get(withClause);
        String endpoint = protocol + "://" + s3uri.endpoint();
        String region = S3Env.getRegion(endpoint, s3uri.bucket());
        return S3.builder()
            .bucket(s3uri.bucket())
            .endpoint(endpoint)
            .accessKeyId(s3uri.accessKey())
            .secretAccessKey(s3uri.secretKey())
            .disableConfigLoad(true)
            .disableEc2Metadata(true)
            .allowAnonymous(true)
            .region(region)
            .build();
    }

    /// Gets the region from the endpoint if well-known and the value contains the region
    /// name, otherwise tries to make a HEAD request
    ///
    /// Adapted from:
    ///     https://github.com/apache/opendal/blob/500532ea92b622d44edd2d84a40e3b80ed5d6e6c/core/services/s3/src/backend.rs?plain=1#L603-L603
    @Nullable
    public static String getRegion(String endpoint, String bucket) {
        Matcher awsMatcher = AWS_ENDPOINT.matcher(endpoint);
        if (awsMatcher.matches()) {
            return awsMatcher.group(1);
        }
        if (endpoint.endsWith("r2.cloudflarestorage.com")) {
            return "auto";
        }
        try (var httpClient = HttpClient.newHttpClient()) {
            HttpRequest request = HttpRequest.newBuilder(URI.create(endpoint + "/" + bucket))
                .HEAD()
                .build();
            try {
                HttpResponse<Void> response = httpClient.send(request, BodyHandlers.discarding());
                Optional<String> bucketRegion = response.headers().firstValue("x-amz-bucket-region");
                if (bucketRegion.isPresent()) {
                    return bucketRegion.get();
                }
                if (response.statusCode() == 403 || response.statusCode() == 200) {
                    return DEFAULT_REGION;
                }
            } catch (IOException | InterruptedException e) {
                LOGGER.warn("Error trying to retrieve region from S3 endpoint", e);
                return null;
            }
        }
        return null;
    }
}
