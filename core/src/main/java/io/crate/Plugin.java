/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;

import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;

/**
 * An extension point allowing to plug in custom functionality.
 * <p>
 * A plugin may implement a constructor with a {@link Settings} argument, which will be called
 * preferred to an empty one.
 * </p>
 */
public abstract class Plugin {

    /**
     * The name of the plugin.
     */
    public abstract String name();

    /**
     * The description of the plugin.
     */
    public abstract String description();

    /**
     * Node level modules (classes, will automatically be created).
     */
    public Collection<Module> nodeModules() {
        return Collections.emptyList();
    }

    /**
     * Node level services that will be automatically started/stopped/closed.
     */
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        return Collections.emptyList();
    }

    /**
     * Per index modules.
     */
    public Collection<Module> indexModules(Settings indexSettings) {
        return Collections.emptyList();
    }

    /**
     * Per index services that will be automatically closed.
     */
    public Collection<Class<? extends Closeable>> indexServices() {
        return Collections.emptyList();
    }

    /**
     * Per index shard module.
     */
    public Collection<Module> shardModules(Settings indexSettings) {
        return Collections.emptyList();
    }

    /**
     * Per index shard service that will be automatically closed.
     */
    public Collection<Class<? extends Closeable>> shardServices() {
        return Collections.emptyList();
    }

    /**
     * Additional node settings loaded by the plugin
     */
    public Settings additionalSettings() {
        return Settings.Builder.EMPTY_SETTINGS;
    }
}
