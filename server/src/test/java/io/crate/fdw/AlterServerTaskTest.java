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

package io.crate.fdw;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class AlterServerTaskTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_cannot_alter_unknown_server() {
        AlterServerRequest request = new AlterServerRequest(
            "server1",
            Settings.EMPTY,
            Settings.EMPTY,
            List.of("key1")
        );
        AlterServerTask task = new AlterServerTask(request);

        assertThatThrownBy(() -> task.execute(clusterService.state()))
            .isExactlyInstanceOf(ResourceNotFoundException.class)
            .hasMessage("Server `server1` not found");
    }

    @Test
    public void test_cannot_add_existing_option() {
        AlterServerRequest request = new AlterServerRequest(
            "server1",
            Settings.builder().put("key1", "value1").build(),
            Settings.EMPTY,
            List.of()
        );

        assertThatThrownBy(() -> AlterServerTask.processOptions(request, Settings.builder().put("key1", "value1")))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Option `key1` already exists for server `server1`, use SET to change it");
    }

    @Test
    public void test_cannot_set_non_existing_option() {
        AlterServerRequest request = new AlterServerRequest(
            "server1",
            Settings.EMPTY,
            Settings.builder().put("key1", "value1").build(),
            List.of()
        );

        assertThatThrownBy(() -> AlterServerTask.processOptions(request, Settings.builder()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Option `key1` does not exist for server `server1`, use ADD to add it");
    }

    @Test
    public void test_cannot_drop_non_existing_option() {
        AlterServerRequest request = new AlterServerRequest(
            "server1",
            Settings.EMPTY,
            Settings.EMPTY,
            List.of("key1")
        );

        assertThatThrownBy(() -> AlterServerTask.processOptions(request, Settings.builder()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Option `key1` does not exist for server `server1`");
    }
}
