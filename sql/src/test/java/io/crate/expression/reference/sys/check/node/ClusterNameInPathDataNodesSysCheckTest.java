/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.reference.sys.check.node;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.env.NodeEnvironment;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class ClusterNameInPathDataNodesSysCheckTest extends CrateDummyClusterServiceUnitTest {
    @Test
    public void validatePathsWithoutClusterNameFolder() throws Exception {
        List<String> paths = new ArrayList<>();
        paths.add("/absolute/path");
        paths.add("./relative-path");

        List<String> clusterNamePaths = ClusterNameInPathDataNodesSysCheck.comparePathDataWithNodeDataPaths(
            "testcluster",
            paths,
            convertToNodeEnvironmentPaths(paths, null));

        assertThat(clusterNamePaths.size(), is(0));
    }

    @Test
    public void validatePathsWithClusterNameFolder() throws Exception {
        List<String> paths = new ArrayList<>();
        paths.add("/absolute/path");
        paths.add("./relative-path");

        List<String> clusterNamePaths = ClusterNameInPathDataNodesSysCheck.comparePathDataWithNodeDataPaths(
            "testcluster",
            paths,
            convertToNodeEnvironmentPaths(paths, "testcluster"));

        assertThat(clusterNamePaths.size(), is(2));
    }

    /**
     * Converts a list of path strings to a Path array. If a clusterName is
     * set the path is going to look like $path/$clustername/nodes/0 such as
     * they are created in NodeEnvironment.
     *
     * @param paths       list of path strings
     * @param clusterName name of the cluster to add to path
     * @return array of paths
     */
    private Path[] convertToNodeEnvironmentPaths(List<String> paths, @Nullable String clusterName) {
        if (clusterName == null) {
            clusterName = "";
        }

        Path[] result = new Path[paths.size()];
        int index = 0;

        for (String path : paths) {
            result[index++] = Paths.get(path, clusterName, NodeEnvironment.NODES_FOLDER, "0");
        }

        return result;
    }
}
