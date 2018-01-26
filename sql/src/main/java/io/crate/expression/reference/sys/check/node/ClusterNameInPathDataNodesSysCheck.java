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

import com.google.common.annotations.VisibleForTesting;
import io.crate.expression.reference.sys.check.AbstractSysCheck;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

@Singleton
// TODO-ES6: REMOVE THIS CHECK ONCE CrateDB IS UPDATED TO ES >= 6.0
public class ClusterNameInPathDataNodesSysCheck extends AbstractSysNodeCheck {

    private final NodeEnvironment nodeEnvironment;
    private final Settings settings;

    static final int ID = 1000;
    private static final String DESCRIPTION = "The path.data directories [%s] appear to have an old structure, which " +
                                              "will not be supported in future versions of CrateDB. %s%d";
    private final List<String> clusterNamePaths;

    @Inject
    public ClusterNameInPathDataNodesSysCheck(ClusterService clusterService,
                                              Settings settings,
                                              NodeEnvironment nodeEnvironment) {
        super(ID, DESCRIPTION, Severity.MEDIUM, clusterService);
        this.nodeEnvironment = nodeEnvironment;
        this.settings = settings;

        // Find all paths that contain the cluster name.
        this.clusterNamePaths = getClusterNamePaths();
    }

    @Override
    public boolean validate() {
        return clusterNamePaths.size() == 0;
    }

    @Override
    public BytesRef description() {
        String linkedDescriptionBuilder = String.format(
            Locale.ENGLISH,
            DESCRIPTION,
            String.join(",", clusterNamePaths),
            AbstractSysCheck.LINK_PATTERN,
            ID);
        return new BytesRef(linkedDescriptionBuilder);
    }

    /**
     * Returns all path.data paths that still use cluster name folder.
     *
     * @return list of all paths that contain the cluster name as folder
     */
    private List<String> getClusterNamePaths() {
        String clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings).value();
        List<String> pathData = Environment.PATH_DATA_SETTING.get(settings);

        return comparePathDataWithNodeDataPaths(clusterName, pathData, nodeEnvironment.nodeDataPaths());
    }

    /**
     * Checks if pathData paths do contain cluster name as a folder. This is
     * done by matching against the NodeEnvironment paths, since this is the
     * only hint, that the fallback procedure is operating.
     *
     * @param clusterName         current cluster name
     * @param configuredDataPaths all paths specified in path.data
     * @param resolvedDataPaths   all paths specified in NodeEnvironment
     * @return list of all paths that contain the cluster name as folder
     */
    @VisibleForTesting
    static List<String> comparePathDataWithNodeDataPaths(String clusterName,
                                                         List<String> configuredDataPaths,
                                                         Path[] resolvedDataPaths) {
        List<String> resolvedDataPathList = trimNodeSuffix(resolvedDataPaths);
        List<String> result = new ArrayList<>();
        int index;
        String path;
        for (String pathDataPath : configuredDataPaths) {
            path = Paths.get(pathDataPath, clusterName).toString();
            index = resolvedDataPathList.indexOf(path);

            if (index != -1) {
                result.add(pathDataPath);
                resolvedDataPathList.remove(index);
                continue;
            }
        }

        return result;
    }

    /**
     * Creates list of strings from NodeEnvironment paths and cuts off nodes folder.
     *
     * @return list of node data paths without nodes suffix
     */
    private static List<String> trimNodeSuffix(Path[] paths) {
        List<String> nodeDataPaths = new ArrayList<>();
        String pathString;
        for (Path path : paths) {
            pathString = path.toString();
            // Cut string ending "/nodes/0"
            nodeDataPaths.add(pathString.substring(0, pathString.indexOf(NodeEnvironment.NODES_FOLDER) - 1));
        }

        return nodeDataPaths;
    }
}
