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

package org.elasticsearch.plugins;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.SettingUpgrader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * An extension point allowing to plug in custom functionality. This class has a number of extension points that are available to all
 * plugins, in addition you can implement any of the following interfaces to further customize CrateDB:
 * <ul>
 * <li>{@link ActionPlugin}
 * <li>{@link AnalysisPlugin}
 * <li>{@link ClusterPlugin}
 * <li>{@link DiscoveryPlugin}
 * <li>{@link MapperPlugin}
 * <li>{@link NetworkPlugin}
 * <li>{@link RepositoryPlugin}
 * <li>{@link SearchPlugin}
 * <li>{@link ReloadablePlugin}
 * </ul>
 */
public abstract class Plugin implements Closeable {

    /**
     * Node level guice modules.
     */
    public Collection<Module> createGuiceModules() {
        return Collections.emptyList();
    }

    /**
     * Node level services that will be automatically started/stopped/closed. This classes must be constructed
     * by injection with guice.
     */
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return Collections.emptyList();
    }

    /**
     * Returns components added by this plugin.
     *
     * Any components returned that implement {@link LifecycleComponent} will have their lifecycle managed.
     * Note: To aid in the migration away from guice, all objects returned as components will be bound in guice
     * to themselves.
     *
     * @param client A client to make requests to the system
     * @param clusterService A service to allow watching and updating cluster state
     * @param threadPool A service to allow retrieving an executor to run an async action
     * @param xContentRegistry the registry for extensible xContent parsing
     * @param environment the environment for path and setting configurations
     * @param nodeEnvironment the node environment used coordinate access to the data paths
     * @param namedWriteableRegistry the registry for {@link NamedWriteable} object parsing
     * @param repositoriesServiceSupplier A supplier for the service that manages snapshot repositories; will return null when this method
     *                                   is called, but will return the repositories service once the node is initialized.
     */
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry,
                                               Supplier<RepositoriesService> repositoriesServiceSupplier) {
        return Collections.emptyList();
    }

    /**
     * Additional node settings loaded by the plugin. Note that settings that are explicit in the nodes settings can't be
     * overwritten with the additional settings. These settings added if they don't exist.
     */
    public Settings additionalSettings() {
        return Settings.EMPTY;
    }

    /**
     * Returns parsers for {@link NamedWriteable} this plugin will use over the transport protocol.
     * @see NamedWriteableRegistry
     */
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Collections.emptyList();
    }

    /**
     * Returns parsers for named objects this plugin will parse from {@link XContentParser#namedObject(Class, String, Object)}.
     * @see NamedWriteableRegistry
     */
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return Collections.emptyList();
    }

    /**
     * Called before a new index is created on a node. The given module can be used to register index-level
     * extensions.
     */
    public void onIndexModule(IndexModule indexModule) {
    }

    /**
     * Returns a list of additional {@link Setting} definitions for this plugin.
     */
    public List<Setting<?>> getSettings() {
        return Collections.emptyList();
    }

    /**
     * Get the setting upgraders provided by this plugin.
     *
     * @return the settings upgraders
     */
    public List<SettingUpgrader<?>> getSettingUpgraders() {
        return Collections.emptyList();
    }

    /**
     * Returns a list of checks that are enforced when a node starts up once a node has the transport protocol bound to a non-loopback
     * interface. In this case we assume the node is running in production and all bootstrap checks must pass. This allows plugins
     * to provide a better out of the box experience by pre-configuring otherwise (in production) mandatory settings or to enforce certain
     * configurations like OS settings or 3rd party resources.
     */
    public List<BootstrapCheck> getBootstrapChecks() {
        return Collections.emptyList();
    }

    public Set<DiscoveryNodeRole> getRoles() {
        return Set.of();
    }

    /**
     * Close the resources opened by this plugin.
     *
     * @throws IOException if the plugin failed to close its resources
     */
    @Override
    public void close() throws IOException {

    }
}
