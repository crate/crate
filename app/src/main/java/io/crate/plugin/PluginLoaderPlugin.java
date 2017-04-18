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

import com.google.common.annotations.VisibleForTesting;
import io.crate.module.PluginLoaderModule;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.*;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.security.KeyStore;
import java.util.*;
import java.util.function.Supplier;

public class PluginLoaderPlugin extends Plugin implements ActionPlugin, MapperPlugin, ClusterPlugin {

    private static final Logger LOGGER = Loggers.getLogger(PluginLoaderPlugin.class);

    @VisibleForTesting
    final PluginLoader pluginLoader;

    @VisibleForTesting
    final Settings settings;

    private final SQLPlugin sqlPlugin;
    private final Settings additionalSettings;
    private final List<Setting<?>> settingList = new ArrayList<>();

    public PluginLoaderPlugin(Settings settings) {
        pluginLoader = new PluginLoader(settings);
        this.settings = Settings.builder()
            .put(pluginLoader.additionalSettings())
            .put(settings)
            .build();
        // SQLPlugin contains modules which use settings which may be overwritten by CratePlugins,
        // so the SQLPLugin needs to be created here with settings that incl. pluginLoader.additionalSettings
        sqlPlugin = new SQLPlugin(this.settings);
        additionalSettings = Settings.builder()
            .put(pluginLoader.additionalSettings())
            .put(sqlPlugin.additionalSettings()).build();

        settingList.add(PluginLoader.SETTING_CRATE_PLUGINS_PATH);
        settingList.addAll(pluginLoader.getSettings());
        settingList.addAll(sqlPlugin.getSettings());

        try {
            initializeTrustStore();
        } catch (Exception e) {
            LOGGER.error("Failed initializing TrustStore: {}", e.getMessage());
        }
    }

    @Override
    public Settings additionalSettings() {
        return additionalSettings;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return settingList;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        Collection<Class<? extends LifecycleComponent>> nodeServices = new ArrayList<>();
        nodeServices.addAll(sqlPlugin.getGuiceServiceClasses());
        nodeServices.addAll(pluginLoader.getGuiceServiceClasses());
        return nodeServices;
    }

    @Override
    public Collection<Module> createGuiceModules() {
        Collection<Module> modules = new ArrayList<>();
        modules.add(new PluginLoaderModule(pluginLoader));
        modules.addAll(pluginLoader.createGuiceModules());
        modules.addAll(sqlPlugin.createGuiceModules());
        return modules;
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController,
                                             ClusterSettings clusterSettings, IndexScopedSettings indexScopedSettings,
                                             SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        List<RestHandler> restHandlers = new ArrayList<>();
        restHandlers.addAll(sqlPlugin.getRestHandlers(settings, restController, clusterSettings,
            indexScopedSettings, settingsFilter, indexNameExpressionResolver, nodesInCluster));
        return restHandlers;
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        Map<String, Mapper.TypeParser> mappers = new HashMap<>();
        mappers.putAll(sqlPlugin.getMappers());
        return mappers;
    }

    @Override
    public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
        return sqlPlugin.createAllocationDeciders(settings, clusterSettings);
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        sqlPlugin.onIndexModule(indexModule);
    }


    public void onModule(Module module) {
        pluginLoader.processModule(module);
    }

    /*
     * Initialize our own TrustStore including StartCom CA
     *
     * TrustStore was generated by:
     *  1. copy cacerts from $JAVA_HOME/jre/lib/security/cacerts
     *  2. add crate.io certificate
     *     keytool -importcert -file crate.io.crt -alias crate -keystore cacerts -storepass changeit
     *
     * see also: http://www.cloudera.com/content/cloudera/en/documentation/core/v5-3-x/topics/cm_sg_create_key_trust.html
     */
    private void initializeTrustStore() throws Exception {
        String trustStorePath = "/ssl/truststore";
        String trustPassword = "changeit";

        // load our key store as a stream and initialize a KeyStore
        try (InputStream trustStream = this.getClass().getResourceAsStream(trustStorePath)) {
            if (trustStream == null) {
                throw new FileNotFoundException("Resource [" + trustStorePath + "] not found in classpath");
            }
            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());

            // load the stream to our store
            trustStore.load(trustStream, trustPassword.toCharArray());

            // initialize a trust manager factory with the trusted store
            TrustManagerFactory trustFactory =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustFactory.init(trustStore);

            // get the trust managers from the factory
            TrustManager[] trustManagers = trustFactory.getTrustManagers();

            // initialize an ssl context to use these managers and set as default
            SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, trustManagers, null);
            SSLContext.setDefault(sslContext);
        }
    }
}
