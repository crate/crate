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

package io.crate.plugin;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.crate.Plugin;
import io.crate.core.CrateComponentLoader;
import org.apache.xbean.finder.ResourceFinder;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.common.io.FileSystemUtils.isAccessibleDirectory;

public class PluginLoader {

    private static final String RESOURCE_PATH = "META-INF/services/";

    private final Settings settings;
    private final Environment environment;
    private final List<Plugin> plugins;
    private final ImmutableMap<Plugin, List<CrateComponentLoader.OnModuleReference>> onModuleReferences;
    private final ESLogger logger;

    public PluginLoader(Settings settings) {
        this.settings = settings;
        environment = new Environment(settings);
        logger = Loggers.getLogger(getClass().getPackage().getName(), settings);

        loadPluginsIntoClassLoader();

        ResourceFinder finder = new ResourceFinder(RESOURCE_PATH);
        List<Class<? extends Plugin>> implementations = null;
        try {
            implementations = finder.findAllImplementations(Plugin.class);
        } catch (ClassCastException e) {
            logger.error("plugin does implement io.crate.Plugin interface", e);
        } catch (ClassNotFoundException e) {
            logger.error("error while loading plugin, misconfigured plugin", e);
        } catch (Exception e) {
            logger.error("error while loading plugins", e);
        }

        if (implementations == null) {
            plugins = ImmutableList.of();
            onModuleReferences = ImmutableMap.of();
            return;
        }

        MapBuilder<Plugin, List<CrateComponentLoader.OnModuleReference>> onModuleReferences = MapBuilder.newMapBuilder();
        ImmutableList.Builder<Plugin> builder = ImmutableList.builder();
        for (Class<? extends Plugin> pluginClass : implementations) {
            Plugin plugin = loadPlugin(pluginClass);
            builder.add(plugin);
            List<CrateComponentLoader.OnModuleReference> onModuleReferenceList = loadModuleReferences(plugin);
            if (!onModuleReferenceList.isEmpty()) {
                onModuleReferences.put(plugin, onModuleReferenceList);
            }
        }
        plugins = builder.build();
        this.onModuleReferences = onModuleReferences.immutableMap();

        if (logger.isInfoEnabled()) {
            logger.info("plugins loaded: {} ", Lists.transform(plugins, new Function<Plugin, String>() {
                @Override
                public String apply(Plugin input) {
                    return input.name();
                }
            }));
        }
    }

    @SuppressWarnings("unchecked")
    private void loadPluginsIntoClassLoader() {
        File pluginsDirectory = environment.pluginsFile();
        if (!isAccessibleDirectory(pluginsDirectory, logger)) {
            return;
        }

        ClassLoader classLoader = settings.getClassLoader();
        Class classLoaderClass = classLoader.getClass();
        Method addURL = null;
        while (!classLoaderClass.equals(Object.class)) {
            try {
                addURL = classLoaderClass.getDeclaredMethod("addURL", URL.class);
                addURL.setAccessible(true);
                break;
            } catch (NoSuchMethodException e) {
                // no method, try the parent
                classLoaderClass = classLoaderClass.getSuperclass();
            }
        }
        if (addURL == null) {
            logger.debug("failed to find addURL method on classLoader [" + classLoader + "] to add methods");
            return;
        }

        File[] plugins = pluginsDirectory.listFiles();
        if (plugins == null) {
            return;
        }

        for (File plugin : plugins) {
            if (!plugin.canRead()) {
                logger.debug("[{}] is not readable.", plugin.getAbsolutePath());
                continue;
            }

            logger.trace("--- adding plugin [{}]", plugin.getAbsolutePath());


            try {
                // add the root
                addURL.invoke(classLoader, plugin.toURI().toURL());
                if (plugin.isFile()) {
                    continue;
                }

                // gather files to add
                List<File> libFiles = Lists.newArrayList();
                File[] pluginFiles = plugin.listFiles();
                if (pluginFiles != null) {
                    libFiles.addAll(Arrays.asList(pluginFiles));
                }
                File libLocation = new File(plugin, "lib");
                if (libLocation.exists() && libLocation.isDirectory()) {
                    File[] pluginLibFiles = libLocation.listFiles();
                    if (pluginLibFiles != null) {
                        libFiles.addAll(Arrays.asList(pluginLibFiles));
                    }
                }

                // if there are jars in it, add it as well
                for (File libFile : libFiles) {
                    if (!(libFile.getName().endsWith(".jar") || libFile.getName().endsWith(".zip"))) {
                        continue;
                    }
                    addURL.invoke(classLoader, libFile.toURI().toURL());
                }
            } catch (Throwable e) {
                logger.warn("failed to add plugin [" + plugin + "]", e);
            }
        }
    }


    private Plugin loadPlugin(Class<? extends Plugin> pluginClass) {
        Constructor<? extends Plugin> constructor;
        try {
            constructor = pluginClass.getConstructor(Settings.class);
            try {
                return constructor.newInstance(settings);
            } catch (Exception e) {
                throw new PluginException("Failed to create plugin [" + pluginClass + "]", e);
            }
        } catch (NoSuchMethodException e) {
            try {
                constructor = pluginClass.getConstructor();
                try {
                    return constructor.newInstance();
                } catch (Exception e1) {
                    throw new PluginException("Failed to create plugin [" + pluginClass + "]", e);
                }
            } catch (NoSuchMethodException e1) {
                throw new PluginException("No constructor for [" + pluginClass + "]");
            }
        }

    }

    private List<CrateComponentLoader.OnModuleReference> loadModuleReferences(Plugin plugin) {
        List<CrateComponentLoader.OnModuleReference> list = Lists.newArrayList();
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
            list.add(new CrateComponentLoader.OnModuleReference(moduleClass, method));
        }

        return list;
    }

    public Collection<Class<? extends Module>> modules() {
        List<Class<? extends Module>> modules = Lists.newArrayList();
        for (Plugin plugin : plugins) {
            modules.addAll(plugin.modules());
        }
        return modules;
    }

    public Collection<Class<? extends LifecycleComponent>> services() {
        List<Class<? extends LifecycleComponent>> services = Lists.newArrayList();
        for (Plugin plugin : plugins) {
            services.addAll(plugin.services());
        }
        return services;
    }

    public Collection<Class<? extends Module>> indexModules() {
        Collection<Class<? extends Module>> modules = new ArrayList<>();
        for (Plugin plugin : plugins) {
            modules.addAll(plugin.indexModules());
        }
        return modules;
    }

    public Collection<Module> modules(Settings settings) {
        List<Module> modules = Lists.newArrayList();
        for (Plugin plugin : plugins) {
            modules.addAll(plugin.modules(settings));
        }
        return modules;
    }

    public Collection<Class<? extends Module>> shardModules() {
        List<Class<? extends Module>> modules = Lists.newArrayList();
        for (Plugin plugin : plugins) {
            modules.addAll(plugin.shardModules());
        }
        return modules;
    }

    public Collection<Module> shardModules(Settings settings) {
        List<Module> modules = Lists.newArrayList();
        for (Plugin plugin : plugins) {
            modules.addAll(plugin.shardModules(settings));
        }
        return modules;
    }


    public Settings additionalSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder();
        for (Plugin plugin : plugins) {
            builder.put(plugin.additionalSettings());
        }
        return builder.build();
    }

    public void processModule(Module module) {
        for (Plugin plugin : plugins) {
            plugin.processModule(module);
            // see if there are onModule references
            List<CrateComponentLoader.OnModuleReference> references = onModuleReferences.get(plugin);
            if (references != null) {
                for (CrateComponentLoader.OnModuleReference reference : references) {
                    if (reference.moduleClass.isAssignableFrom(module.getClass())) {
                        try {
                            reference.onModuleMethod.invoke(plugin, module);
                        } catch (Exception e) {
                            logger.warn("Plugin {}, failed to invoke custom onModule method", e, plugin.name());
                        }
                    }
                }
            }
        }
    }


}
