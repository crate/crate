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

package io.crate.execution.jobs;


import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.common.concurrent.ConcurrencyLimit;


public class NodeLimitsTest extends ESTestCase {

    private ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    private NodeLimits nodeLimits = new NodeLimits(clusterSettings);

    @Test
    public void test_can_receive_limits_for_null_node() {
        ConcurrencyLimit limit = nodeLimits.get(null);
        assertThat(limit).isNotNull();
    }

    @Test
    public void test_successive_calls_return_same_instance_for_same_node() {
        assertThat(nodeLimits.get(null)).isSameAs(nodeLimits.get(null));
        assertThat(nodeLimits.get("n1")).isSameAs(nodeLimits.get("n1"));
        assertThat(nodeLimits.get("n1")).isNotSameAs(nodeLimits.get("n2"));
    }

    @Test
    public void test_updating_node_limit_settings_results_in_new_concurrency_limit_instances() throws Exception {
        ConcurrencyLimit limit = nodeLimits.get("n1");
        clusterSettings.applySettings(Settings.builder()
            .put("overload_protection.dml.initial_concurrency", 10)
            .build()
        );
        ConcurrencyLimit updatedLimit = nodeLimits.get("n1");
        assertThat(limit).isNotSameAs(updatedLimit);
        assertThat(limit.getLimit()).isEqualTo(5);
        assertThat(updatedLimit.getLimit()).isEqualTo(10);
    }
}
