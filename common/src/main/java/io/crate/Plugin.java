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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * An extension point allowing to plug in custom functionality.
 * <p>
 * A plugin may implement a constructor with a {@link Settings} argument, which will be called
 * preferred to an empty one.
 * </p>
 */
public interface Plugin {

    /**
     * The name of the plugin.
     */
    String name();

    /**
     * The description of the plugin.
     */
    String description();

    /**
     * Node level guice modules.
     */
    default Collection<Module> createGuiceModules() {
        return Collections.emptyList();
    }

    /**
     * Node level services that will be automatically started/stopped/closed. This classes must be constructed
     * by injection with guice.
     */
    default Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return Collections.emptyList();
    }

    /**
     * Additional node settings loaded by the plugin
     */
    default Settings additionalSettings() {
        return Settings.Builder.EMPTY_SETTINGS;
    }

    /**
     * Returns a list of additional {@link Setting} definitions for this plugin.
     */
    default List<Setting<?>> getSettings() {
        return Collections.emptyList();
    }
}
