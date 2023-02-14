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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.elasticsearch.gateway.PersistedClusterStateService.overrideVersion;

import java.nio.file.Path;

import org.assertj.core.api.AbstractThrowableAssert;
import org.elasticsearch.Version;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.TestCluster;
import org.junit.Test;


@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0)
public class NodeEnvironmentIT extends IntegTestCase {

    private AbstractThrowableAssert<?, ? extends Throwable> expectThrowsOnRestart(CheckedConsumer<Path[], Exception> onNodeStopped) {
        cluster().startNode();
        final Path[] dataPaths = cluster().getInstance(NodeEnvironment.class).nodeDataPaths();

        return assertThatThrownBy(() -> {
            cluster().restartRandomDataNode(new TestCluster.RestartCallback() {
                @Override
                public Settings onNodeStopped(String nodeName) {
                    try {
                        onNodeStopped.accept(dataPaths);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                    return Settings.EMPTY;
                }
            });
        }).isExactlyInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testFailsToStartIfDowngraded() {
        expectThrowsOnRestart(dataPaths -> overrideVersion(NodeMetadataTests.tooNewVersion(), dataPaths))
            .hasMessageStartingWith("cannot downgrade a node from version [")
            .hasMessageEndingWith("] to version [" + Version.CURRENT + "]");
    }

    @Test
    public void testFailsToStartIfUpgradedTooFar() {
        expectThrowsOnRestart(dataPaths -> overrideVersion(NodeMetadataTests.tooOldVersion(), dataPaths))
            .hasMessageStartingWith("cannot upgrade a node from version [")
            .hasMessageEndingWith("] directly to version [" + Version.CURRENT + "]");
    }
}
