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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.opendal.ServiceConfig.Gcs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class GCSRepositoryTest extends ESTestCase {

    @Test
    public void test_settings_to_gcs_config() throws Exception {
        Settings settings = Settings.builder()
            .put("project_id", "p1")
            .put("private_key_id", "secret_key_id")
            .put("private_key", "secret_key")
            .put("client_id", "c1")
            .put("client_email", "user@example.com")
            .build();
        Gcs config = GCSRepository.createConfig(settings);
        assertThat(config.getDisableVmMetadata()).isNull();
        assertThat(config.getDisableConfigLoad()).isNull();
        assertThat(config.getCredential()).isEqualTo(
            "eyJ0eXBlIjoic2VydmljZV9hY2NvdW50IiwicHJvamVjdF9pZCI6InAxIiwicHJpdmF0ZV9rZXlfaWQiOiJzZWNyZXRfa2V5X2lkIiwicHJpdmF0ZV9rZXkiOiJzZWNyZXRfa2V5IiwiY2xpZW50X2lkIjoiYzEiLCJjbGllbnRfZW1haWwiOiJ1c2VyQGV4YW1wbGUuY29tIiwiYXV0aF91cmkiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20vby9vYXV0aDIvYXV0aCIsInRva2VuX3VyaSI6Imh0dHBzOi8vb2F1dGgyLmdvb2dsZWFwaXMuY29tL3Rva2VuIiwiYXV0aF9wcm92aWRlcl94NTA5X2NlcnRfdXJsIjoiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vb2F1dGgyL3YxL2NlcnRzIn0=");
    }
}
