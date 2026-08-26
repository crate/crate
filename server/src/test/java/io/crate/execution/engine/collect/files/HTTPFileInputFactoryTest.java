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

package io.crate.execution.engine.collect.files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.http.HttpClient.Redirect;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.Mockito;

public class HTTPFileInputFactoryTest {

    @Test
    public void test_redirect_setting_value_validation() throws Exception {
        var clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        Settings settings = Settings.builder()
            .put(HTTPFileInputFactory.REDIRECT_SETTING.getKey(), "foo")
            .build();
        assertThatThrownBy(() -> clusterSettings.validate(settings, false))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid redirects value 'foo'. Expected one of [normal, always, never]");
    }

    @Test
    public void test_builds_http_client_with_redirect_based_on_setting() throws Exception {
        var settings = Settings.builder()
            .put(HTTPFileInputFactory.REDIRECT_SETTING.getKey(), "always")
            .build();
        var clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        var httpFileInputFactory = new HTTPFileInputFactory(
            clusterSettings,
            Mockito.mock(ThreadPool.class, Answers.RETURNS_MOCKS)
        );
        assertThat(httpFileInputFactory.client.followRedirects()).isEqualTo(Redirect.ALWAYS);

        clusterSettings.applySettings(Settings.builder()
            .put(HTTPFileInputFactory.REDIRECT_SETTING.getKey(), "never")
            .build()
        );
        assertThat(httpFileInputFactory.client.followRedirects()).isEqualTo(Redirect.NEVER);
    }
}

