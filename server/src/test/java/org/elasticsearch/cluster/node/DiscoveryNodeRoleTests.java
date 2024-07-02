/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.node;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class DiscoveryNodeRoleTests extends ESTestCase {

    @Test
    public void testDiscoveryNodeSetPossibleRolesRejectsDuplicateRoleNames() {
        assertThatThrownBy(() -> DiscoveryNode.setPossibleRoles(Set.of(
                        new DiscoveryNodeRole("foo", "f") {

                            @Override
                            protected Setting<Boolean> roleSetting() {
                                return null;
                            }

                        },
                        new DiscoveryNodeRole("foo", "f") {

                            @Override
                            protected Setting<Boolean> roleSetting() {
                                return null;
                            }

                        }))
        ).isExactlyInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Duplicate key foo ");
    }

    @Test
    public void testDiscoveryNodeSetPossibleRolesRejectsDuplicateRoleNameAbbreviations() {
        assertThatThrownBy(() -> DiscoveryNode.setPossibleRoles(Set.of(
                        new DiscoveryNodeRole("foo_1", "f") {

                            @Override
                            protected Setting<Boolean> roleSetting() {
                                return null;
                            }

                        },
                        new DiscoveryNodeRole("foo_2", "f") {

                            @Override
                            protected Setting<Boolean> roleSetting() {
                                return null;
                            }

                        }))
        ).isExactlyInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Duplicate key f ");
    }

}
