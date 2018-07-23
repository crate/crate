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

package io.crate.plugin;

import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.test.CauseMatcher;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class PluginLoaderTest extends ESIntegTestCase {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(PluginLoaderPlugin.class);
    }

    @Test
    public void testLoadPlugin() throws Exception {
        String node = startNodeWithPlugins("/io/crate/plugin/simple_plugin");

        PluginsService pluginsService = internalCluster().getInstance(PluginsService.class, node);
        PluginLoaderPlugin corePlugin = getCratePlugin(pluginsService);

        PluginLoader pluginLoader = corePlugin.pluginLoader;
        assertThat(pluginLoader.plugins.size(), is(1));
        assertThat(pluginLoader.plugins.get(0).getClass().getCanonicalName(), is("io.crate.plugin.ExamplePlugin"));
    }

    @Test
    public void testPluginWithCrateSettings() throws Exception {
        String node = startNodeWithPlugins("/io/crate/plugin/plugin_with_crate_settings");

        PluginsService pluginsService = internalCluster().getInstance(PluginsService.class, node);
        PluginLoaderPlugin corePlugin = getCratePlugin(pluginsService);
        Settings settings = corePlugin.settings;

        assertThat(settings.get("setting.for.crate"), is("foo"));
    }

    @Test
    public void testLoadPluginRegisteringScalarFunction() throws Exception {
        String node = startNodeWithPlugins("/io/crate/plugin/simple_plugin_registering_scalar_function");

        PluginsService pluginsService = internalCluster().getInstance(PluginsService.class, node);
        PluginLoaderPlugin corePlugin = getCratePlugin(pluginsService);

        PluginLoader pluginLoader = corePlugin.pluginLoader;
        assertThat(pluginLoader.plugins.size(), is(1));
        assertThat(pluginLoader.plugins.get(0).getClass().getCanonicalName(), is("io.crate.plugin.ExamplePlugin"));

        Functions functions = internalCluster().getInstance(Functions.class);
        assertThat(functions.getBuiltin("is_even", Collections.singletonList(DataTypes.LONG)).info(),
                   is(new FunctionInfo(
                       new FunctionIdent("is_even", Collections.singletonList(DataTypes.LONG)),
                       DataTypes.BOOLEAN)));

        // Also check that the built-in functions are not lost
        assertThat(functions.getBuiltin("abs", Collections.singletonList(DataTypes.LONG)).info(),
                   is(new FunctionInfo(
                       new FunctionIdent("abs", Collections.singletonList(DataTypes.LONG)),
                       DataTypes.LONG)));
    }

    @Test
    public void testLoadPluginWithAlreadyLoadedClass() throws Exception {
        // test that JarHell is used and plugin is not loaded because it contains an already loaded class
        expectedException.expect(CauseMatcher.causeOfCause(RuntimeException.class));
        startNodeWithPlugins("/io/crate/plugin/plugin_with_already_loaded_class");
    }

    @Test
    public void testDuplicates() throws Exception {
        // test that node will die due to jarHell (same plugin jar loaded twice)
        expectedException.expect(CauseMatcher.causeOfCause(RuntimeException.class));
        startNodeWithPlugins("/io/crate/plugin/duplicates");
    }

    @Test
    public void testInvalidPluginEmptyDirectory() throws Exception {
        // test that node will die because of an invalid plugin (in this case, just an empty directory)
        expectedException.expect(CauseMatcher.causeOfCause(RuntimeException.class));
        startNodeWithPlugins("/io/crate/plugin/invalid");
    }


    private static String startNodeWithPlugins(String pluginDir) throws URISyntaxException {
        URL resource = PluginLoaderTest.class.getResource(pluginDir);
        Settings.Builder settings = Settings.builder();
        if (resource != null) {
            settings.put(PluginLoader.SETTING_CRATE_PLUGINS_PATH.getKey(), new File(resource.toURI()).getAbsolutePath());
        }
        String nodeName = internalCluster().startNode(settings);
        // We wait for a Green status
        client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        return internalCluster().getInstance(ClusterService.class, nodeName).state().nodes().getLocalNode().getName();
    }

    private PluginLoaderPlugin getCratePlugin(PluginsService pluginsService) {
        // find the PluginLoaderPlugin, should be the only one loaded
        List<PluginLoaderPlugin> pluginLoaderPlugins = pluginsService.filterPlugins(PluginLoaderPlugin.class);
        if (pluginLoaderPlugins.isEmpty()) {
            throw new IllegalStateException("Couldn't find PluginLoaderPlugin");
        }
        return pluginLoaderPlugins.get(0);
    }
}
