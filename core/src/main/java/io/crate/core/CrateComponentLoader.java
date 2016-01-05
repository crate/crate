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

package io.crate.core;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.crate.CrateComponent;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;

public class CrateComponentLoader {

    private static CrateComponentLoader INSTANCE;

    private final ArrayList<Plugin> plugins;
    private static final ESLogger logger = Loggers.getLogger(CrateComponentLoader.class);
    private final ImmutableMap<Plugin, List<OnModuleReference>> onModuleReferences;

    public static synchronized CrateComponentLoader getInstance(Settings settings) {
        if (INSTANCE == null) {
            INSTANCE = new CrateComponentLoader(settings);
        }
        return INSTANCE;
    }

    private CrateComponentLoader(Settings settings) {
        ServiceLoader<CrateComponent> crateComponents = ServiceLoader.load(CrateComponent.class);
        plugins = new ArrayList<>();
        MapBuilder<Plugin, List<OnModuleReference>> onModuleReferences = MapBuilder.newMapBuilder();
        for (CrateComponent crateComponent : crateComponents) {
            logger.trace("Loading crateComponent: {}", crateComponent);
            Plugin plugin = crateComponent.createPlugin(settings);
            plugins.add(plugin);
            List<OnModuleReference> list = Lists.newArrayList();
            for (Method method : plugin.getClass().getDeclaredMethods()) {
                if (!method.getName().equals("onModule")) {
                    continue;
                }
                if (method.getParameterTypes().length == 0 || method.getParameterTypes().length > 1) {
                    logger.warn("Plugin: {} implementing onModule with no parameters or more than one parameter", plugin.name());
                    continue;
                }
                Class moduleClass = method.getParameterTypes()[0];
                if (!Module.class.isAssignableFrom(moduleClass)) {
                    logger.warn("Plugin: {} implementing onModule by the type is not of Module type {}", plugin.name(), moduleClass);
                    continue;
                }
                method.setAccessible(true);
                //noinspection unchecked
                list.add(new OnModuleReference(moduleClass, method));
            }
            if (!list.isEmpty()) {
                onModuleReferences.put(plugin, list);
            }
        }
        this.onModuleReferences = onModuleReferences.immutableMap();
    }

    public Collection<Module> nodeModules() {
        List<Module> modules = Lists.newArrayList();
        for (Plugin plugin : plugins) {
            modules.addAll(plugin.nodeModules());
        }
        return modules;
    }

    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        List<Class<? extends LifecycleComponent>> services = Lists.newArrayList();
        for (Plugin plugin : plugins) {
            services.addAll(plugin.nodeServices());
        }
        return services;
    }

    public Collection<Module> indexModules(Settings indexSettings) {
        Collection<Module> modules = new ArrayList<>();
        for (Plugin plugin : plugins) {
            modules.addAll(plugin.indexModules(indexSettings));
        }
        return modules;
    }

    public Collection<Module> shardModules(Settings indexSettings) {
        List<Module> modules = Lists.newArrayList();
        for (Plugin plugin : plugins) {
            modules.addAll(plugin.shardModules(indexSettings));
        }
        return modules;
    }

    public void processModules(Iterable<Module> modules) {
        for (Module module : modules) {
            processModule(module);
        }
    }

    public void processModule(Module module) {
        for (Plugin plugin : plugins) {
            // see if there are onModule references
            List<OnModuleReference> references = onModuleReferences.get(plugin);
            if (references != null) {
                for (OnModuleReference reference : references) {
                    if (reference.moduleClass.isAssignableFrom(module.getClass())) {
                        try {
                            reference.onModuleMethod.invoke(plugin, module);
                        } catch (Exception e) {
                            logger.warn("plugin {}, failed to invoke custom onModule method", e, plugin.name());
                        }
                    }
                }
            }
        }
    }

    public Settings additionalSettings() {
        Settings.Builder builder = Settings.settingsBuilder();
        for (Plugin plugin : plugins) {
            builder.put(plugin.additionalSettings());
        }
        return builder.build();
    }

    public static class OnModuleReference {
        public final Class<? extends Module> moduleClass;
        public final Method onModuleMethod;

        public OnModuleReference(Class<? extends Module> moduleClass, Method onModuleMethod) {
            this.moduleClass = moduleClass;
            this.onModuleMethod = onModuleMethod;
        }
    }
}
