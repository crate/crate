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

package org.elasticsearch.env;

import io.crate.integrationtests.SQLIntegrationTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Test;

import java.nio.file.Path;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.startsWith;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class NodeEnvironmentIT extends SQLIntegrationTestCase {

    private IllegalStateException expectThrowsOnRestart(CheckedConsumer<Path[], Exception> onNodeStopped) {
        internalCluster().startNode();
        final Path[] dataPaths = internalCluster().getInstance(NodeEnvironment.class).nodeDataPaths();
        return expectThrows(IllegalStateException.class,
                            () -> internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
                                @Override
                                public Settings onNodeStopped(String nodeName) {
                                    try {
                                        onNodeStopped.accept(dataPaths);
                                    } catch (Exception e) {
                                        throw new AssertionError(e);
                                    }
                                    return Settings.EMPTY;
                                }
                            }));
    }

    @Test
    public void testFailsToStartIfDowngraded() {
        final IllegalStateException illegalStateException = expectThrowsOnRestart(
            dataPaths -> PersistedClusterStateService.overrideVersion(NodeMetadataTests.tooNewVersion(), dataPaths)
        );

        assertThat(illegalStateException.getMessage(),
                   allOf(startsWith("cannot downgrade a node from version ["),
                         endsWith("] to version [" + Version.CURRENT + "]")));
    }

    @Test
    public void testFailsToStartIfUpgradedTooFar() {
        final IllegalStateException illegalStateException = expectThrowsOnRestart(
            dataPaths -> PersistedClusterStateService.overrideVersion(NodeMetadataTests.tooOldVersion(), dataPaths)
        );

        assertThat(illegalStateException.getMessage(),
                   allOf(startsWith("cannot upgrade a node from version ["),
                         endsWith("] directly to version [" + Version.CURRENT + "]")));
    }
}
