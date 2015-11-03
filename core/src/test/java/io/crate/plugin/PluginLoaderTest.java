/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
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

package io.crate.plugin;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class PluginLoaderTest extends ElasticsearchIntegrationTest {

    @Test
    public void testLoadPlugin() throws Exception {
        String node = startNodeWithPlugins("/io/crate/plugin/simple_plugin");

        PluginsService pluginsService = internalCluster().getInstance(PluginsService.class, node);
        assertThat(pluginsService.plugins().get(0).v2(), instanceOf(CrateCorePlugin.class));
        CrateCorePlugin corePlugin = (CrateCorePlugin) pluginsService.plugins().get(0).v2();

        PluginLoader pluginLoader = corePlugin.pluginLoader;
        assertThat(pluginLoader.plugins.size(), is(1));
        assertThat(pluginLoader.plugins.get(0).getClass().getCanonicalName(), is("io.crate.plugin.ExamplePlugin"));
    }

    @Test
    public void testLoadPluginWithAlreadyLoadedClass() throws Exception {
        // test that JarHell is used and plugin is not loaded because it contains a already loaded class

        String node = startNodeWithPlugins("/io/crate/plugin/plugin_with_already_loaded_class");
        PluginsService pluginsService = internalCluster().getInstance(PluginsService.class, node);
        CrateCorePlugin corePlugin = (CrateCorePlugin) pluginsService.plugins().get(0).v2();

        PluginLoader pluginLoader = corePlugin.pluginLoader;
        assertThat(pluginLoader.plugins, Matchers.empty());
    }


    private static String startNodeWithPlugins(String pluginDir) throws URISyntaxException {
        URL resource = PluginLoaderTest.class.getResource(pluginDir);
        ImmutableSettings.Builder settings = settingsBuilder();
        if (resource != null) {
            settings.put("path.plugins", new File(resource.toURI()).getAbsolutePath());
        }
        settings.put("plugin.types", CrateCorePlugin.class.getName());

        String nodeName = internalCluster().startNode(settings);

        // We wait for a Green status
        client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();

        return internalCluster().getInstance(ClusterService.class, nodeName).state().nodes().localNode().name();
    }

}
