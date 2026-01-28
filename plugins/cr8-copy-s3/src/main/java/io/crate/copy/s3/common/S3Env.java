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

import org.apache.opendal.ServiceConfig;
import org.elasticsearch.common.settings.Settings;

import io.crate.opendal.S3;

public final class S3Env {

    private S3Env() {
    }

    public static ServiceConfig.S3 getServiceConfig(S3URI s3uri, Settings withClause) {
        String protocol = S3Protocol.get(withClause);
        String endpoint = protocol + "://" + s3uri.endpoint();
        String region = S3.getRegion(endpoint, s3uri.bucket());
        return ServiceConfig.S3.builder()
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
}
