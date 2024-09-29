/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.copy.s3;

import static io.crate.copy.s3.common.S3Settings.PROTOCOL_SETTING;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import io.crate.copy.Configuration;
import io.crate.copy.s3.common.S3URI;

public class S3Configuration implements Configuration<S3URI> {

    @Override
    public List<Setting<String>> supportedSettings() {
        return List.of(PROTOCOL_SETTING);
    }

    @Override
    public Map<String, String> fromURIAndSettings(S3URI s3URI, Settings settings) {
        Map<String, String> config = new HashMap<>();
        String endpoint = String.format(
            Locale.ENGLISH,
            "%s://%s",
            PROTOCOL_SETTING.get(settings),
            s3URI.endpoint()
        );
        config.put("bucket", s3URI.bucket());
        config.put("endpoint", endpoint);
        // OpenDAL requires region property. If it's irrelevant, value 'auto' can be provided.
        // See https://docs.rs/opendal/latest/opendal/services/struct.S3.html#basic-setup
        config.put("region", "auto");
        if (s3URI.secretKey() != null) {
            config.put("secret_access_key", s3URI.secretKey());
        }
        if (s3URI.accessKey() != null) {
            config.put("access_key_id", s3URI.accessKey());
        }
        return config;
    }
}
