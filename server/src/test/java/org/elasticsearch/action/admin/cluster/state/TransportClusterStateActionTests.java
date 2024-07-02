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

package org.elasticsearch.action.admin.cluster.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.action.admin.cluster.state.TransportClusterStateAction.buildResponse;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

public class TransportClusterStateActionTests extends ESTestCase {

    private final Logger logger = LogManager.getLogger(getClass());

    private ClusterState clusterState;


    @Before
    public void setup() throws Throwable {
        clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder()
                .persistentSettings(Settings.builder().put("setting1", "bar").build())
                .put(IndexTemplateMetadata.builder("template1")
                    .patterns(List.of("*"))
                    .putMapping("{\"default\": {}}")
                    .build())
                .put(IndexMetadata.builder("index1")
                         .settings(settings(Version.CURRENT))
                         .numberOfShards(1)
                         .numberOfReplicas(0)
                         .build(),
                     true
                )
                .build()
            )
            .build();
    }

    @Test
    public void test_response_contains_complete_metadata_if_no_indices_or_templates_requested() {
        var request = new ClusterStateRequest();
        request.metadata(true);

        var response = buildResponse(request, clusterState, logger);
        assertThat(response.getState().metadata().templates().get("template1")).isNotNull();
        assertThat(response.getState().metadata().hasIndex("index1")).isTrue();
        assertThat(response.getState().metadata().persistentSettings().get("setting1")).isEqualTo("bar");
    }

    @Test
    public void test_response_contains_templates_only() {
        var request = new ClusterStateRequest();
        request.metadata(true);
        request.templates("template1");

        var response = buildResponse(request, clusterState, logger);
        assertThat(response.getState().metadata().templates().get("template1")).isNotNull();
        assertThat(response.getState().metadata().hasIndex("index1")).isFalse();
        assertThat(response.getState().metadata().persistentSettings().get("setting1")).isNull();
    }

    @Test
    public void test_response_contains_indices_only() {
        var request = new ClusterStateRequest();
        request.metadata(true);
        request.indices("index1");

        var response = buildResponse(request, clusterState, logger);
        assertThat(response.getState().metadata().templates().get("template1")).isNull();
        assertThat(response.getState().metadata().hasIndex("index1")).isTrue();
        assertThat(response.getState().metadata().persistentSettings().get("setting1")).isNull();
    }

    @Test
    public void test_response_contains_indices_and_templates_only() {
        var request = new ClusterStateRequest();
        request.metadata(true);
        request.templates("template1");
        request.indices("index1");

        var response = buildResponse(request, clusterState, logger);
        assertThat(response.getState().metadata().templates().get("template1")).isNotNull();
        assertThat(response.getState().metadata().hasIndex("index1")).isTrue();
        assertThat(response.getState().metadata().persistentSettings().get("setting1")).isNull();
    }
}
