/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.plugin;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import io.crate.core.CrateComponentLoader;
import io.crate.module.CrateCoreModule;
import io.crate.module.CrateCoreShardModule;
import io.crate.rest.CrateRestMainAction;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;


public class CrateCorePlugin extends AbstractPlugin {

    private final Settings settings;
    private final CrateComponentLoader crateComponentLoader;
    @VisibleForTesting
    final PluginLoader pluginLoader;

    private static final ESLogger LOGGER = Loggers.getLogger(CrateCorePlugin.class);

    public CrateCorePlugin(Settings settings) {
        this.settings = settings;
        crateComponentLoader = CrateComponentLoader.getInstance(settings);
        pluginLoader = new PluginLoader(settings);

        try {
            initializeTrustStore();
        } catch (Exception e) {
            LOGGER.error("Failed initializing TrustStore: {}", e.getMessage());
        }
    }

    @Override
    public String name() {
        return "crate-core";
    }

    @Override
    public String description() {
        return "plugin that provides a collection of utilities used in other crate modules.";
    }

    @Override
    public Settings additionalSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        builder.put(crateComponentLoader.additionalSettings());
        builder.put(pluginLoader.additionalSettings());
        return builder.build();
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        Collection<Class<? extends LifecycleComponent>> services = Lists.newArrayList();
        services.addAll(crateComponentLoader.services());
        services.addAll(pluginLoader.services());
        return services;
    }

    @Override
    public Collection<Class<? extends Module>> indexModules() {
        Collection<Class<? extends Module>> indexModules = Lists.newArrayList();
        indexModules.addAll(crateComponentLoader.indexModules());
        indexModules.addAll(pluginLoader.indexModules());
        return indexModules;
    }

    @Override
    public Collection<Module> modules(Settings settings) {
        Collection<Module> modules = new ArrayList<>();
        CrateCoreModule crateCoreModule = new CrateCoreModule(settings, crateComponentLoader, pluginLoader);
        modules.add(crateCoreModule);
        modules.addAll(pluginLoader.modules(settings));
        return modules;
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = new ArrayList<>();
        modules.addAll(crateComponentLoader.modules());
        modules.addAll(pluginLoader.modules());
        return modules;
    }

    @Override
    public Collection<Class<? extends Module>> shardModules() {
        Collection<Class<? extends Module>> modules = new ArrayList<>();
        if (!settings.getAsBoolean("node.client", false)) {
            modules.add(CrateCoreShardModule.class);
        }
        return modules;
    }

    public void onModule(RestModule restModule) {
        restModule.addRestAction(CrateRestMainAction.class);
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
