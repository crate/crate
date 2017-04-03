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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.Plugin;
import org.apache.xbean.finder.ResourceFinder;
import org.elasticsearch.bootstrap.JarHell;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.PluginInfo;

import java.io.Closeable;
import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static org.elasticsearch.common.io.FileSystemUtils.isAccessibleDirectory;

public class PluginLoader {

    private static final String RESOURCE_PATH = "META-INF/services/";

    private final Settings settings;
    private final ImmutableMap<Plugin, List<OnModuleReference>> onModuleReferences;
    private final ESLogger logger;

    @VisibleForTesting
    final List<Plugin> plugins;
    private final Path pluginsPath;
    private final List<URL> jarsToLoad = new ArrayList<>();

    PluginLoader(Settings settings) {
        this.settings = settings;

        String pluginFolder = settings.get("path.crate_plugins");
        if (pluginFolder == null) {
            pluginsPath = PathUtils.get(Strings.cleanPath(settings.get("path.home"))).resolve("plugins");
        } else {
            pluginsPath = PathUtils.get(Strings.cleanPath(pluginFolder));
        }
        logger = Loggers.getLogger(getClass().getPackage().getName(), settings);

        Collection<Class<? extends Plugin>> implementations = findImplementations();

        MapBuilder<Plugin, List<OnModuleReference>> onModuleReferences = MapBuilder.newMapBuilder();
        ImmutableList.Builder<Plugin> builder = ImmutableList.builder();
        for (Class<? extends Plugin> pluginClass : implementations) {
            Plugin plugin;
            try {
                plugin = loadPlugin(pluginClass);
            } catch (Throwable t) {
                logger.error("error loading plugin:  " + pluginClass.getSimpleName(), t);
                continue;
            }
            try {
                List<OnModuleReference> onModuleReferenceList = loadModuleReferences(plugin);
                if (!onModuleReferenceList.isEmpty()) {
                    onModuleReferences.put(plugin, onModuleReferenceList);
                }
            } catch (Throwable t) {
                logger.error("error loading moduleReferences from plugin: " + plugin.name(), t);
                continue;
            }

            builder.add(plugin);
        }
        plugins = builder.build();
        this.onModuleReferences = onModuleReferences.immutableMap();

        if (logger.isInfoEnabled()) {
            logger.info("plugins loaded: {} ", plugins.stream().map(Plugin::name).collect(Collectors.toList()));
        }
    }

    private Collection<Class<? extends Plugin>> findImplementations() {
        if (!isAccessibleDirectory(pluginsPath, logger)) {
            return Collections.emptyList();
        }

        File[] plugins = pluginsPath.toFile().listFiles();
        if (plugins == null) {
            return Collections.emptyList();
        }

        Collection<Class<? extends Plugin>> allImplementations = new ArrayList<>();
        for (File plugin : plugins) {
            if (!plugin.canRead()) {
                logger.debug("[{}] is not readable.", plugin.getAbsolutePath());
                continue;
            }
            // check if its an elasticsearch plugin
            Path esDescriptorFile = plugin.toPath().resolve(PluginInfo.ES_PLUGIN_PROPERTIES);
            try {
                if (esDescriptorFile.toFile().exists()) {
                    continue;
                }
            } catch (Exception e) {
                // ignore
            }

            List<URL> pluginUrls = new ArrayList<>();

            logger.trace("--- adding plugin [{}]", plugin.getAbsolutePath());

            try {
                URL pluginURL = plugin.toURI().toURL();
                // jar-hell check the plugin against the parent classloader
                try {
                    checkJarHell(pluginURL);
                } catch (Exception e) {
                    String msg = String.format(Locale.ENGLISH,
                        "failed to load plugin %s due to jar hell", pluginURL);
                    logger.error(msg, e);
                    throw new RuntimeException(msg, e);
                }
                pluginUrls.add(pluginURL);

                if (!plugin.isFile()) {
                    // gather files to add
                    List<File> libFiles = new ArrayList<>();
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
                        URL libURL = libFile.toURI().toURL();
                        // jar-hell check the plugin lib against the parent classloader
                        try {
                            checkJarHell(libURL);
                            pluginUrls.add(libURL);
                        } catch (Exception e) {
                            String msg = String.format(Locale.ENGLISH,
                                "Library %s of plugin %s already loaded", libURL, pluginURL);
                            logger.error(msg, e);
                            throw new RuntimeException(msg, e);
                        }
                    }
                }
            } catch (MalformedURLException e) {
                String msg = String.format(Locale.ENGLISH, "failed to add plugin [%s]", plugin);
                logger.error(msg, e);
                throw new RuntimeException(msg, e);
            }
            Collection<Class<? extends Plugin>> implementations = findImplementations(pluginUrls);
            if (implementations == null || implementations.isEmpty()) {
                String msg = String.format(Locale.ENGLISH,
                    "Path [%s] does not contain a valid Crate or Elasticsearch plugin", plugin.getAbsolutePath());
                RuntimeException e = new RuntimeException(msg);
                logger.error(msg, e);
                throw e;
            }
            jarsToLoad.addAll(pluginUrls);
            allImplementations.addAll(implementations);
        }
        return allImplementations;
    }

    @Nullable
    private Collection<Class<? extends Plugin>> findImplementations(Collection<URL> pluginUrls) {
        URL[] urls = pluginUrls.toArray(new URL[pluginUrls.size()]);
        ClassLoader loader = URLClassLoader.newInstance(urls, getClass().getClassLoader());

        ResourceFinder finder = new ResourceFinder(RESOURCE_PATH, loader, urls);
        try {
            return finder.findAllImplementations(Plugin.class);
        } catch (ClassCastException e) {
            logger.error("plugin does not implement io.crate.Plugin interface", e);
        } catch (ClassNotFoundException e) {
            logger.error("error while loading plugin, misconfigured plugin", e);
        } catch (Throwable t) {
            logger.error("error while loading plugins", t);
        }

        return null;
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

    private List<OnModuleReference> loadModuleReferences(Plugin plugin) {
        List<OnModuleReference> list = new ArrayList<>();
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

        return list;
    }

    Collection<Module> nodeModules() {
        List<Module> modules = new ArrayList<>();
        for (Plugin plugin : plugins) {
            modules.addAll(plugin.nodeModules());
        }
        return modules;
    }

    Collection<Class<? extends LifecycleComponent>> nodeServices() {
        List<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        for (Plugin plugin : plugins) {
            services.addAll(plugin.nodeServices());
        }
        return services;
    }

    Collection<Module> indexModules(Settings indexSettings) {
        List<Module> modules = new ArrayList<>();
        for (Plugin plugin : plugins) {
            modules.addAll(plugin.indexModules(indexSettings));
        }
        return modules;
    }

    Collection<Class<? extends Closeable>> indexServices() {
        List<Class<? extends Closeable>> services = new ArrayList<>();
        for (Plugin plugin : plugins) {
            services.addAll(plugin.indexServices());
        }
        return services;
    }

    Collection<Module> shardModules(Settings indexSettings) {
        List<Module> modules = new ArrayList<>();
        for (Plugin plugin : plugins) {
            modules.addAll(plugin.shardModules(indexSettings));
        }
        return modules;
    }

    Collection<Class<? extends Closeable>> shardServices() {
        List<Class<? extends Closeable>> services = new ArrayList<>();
        for (Plugin plugin : plugins) {
            services.addAll(plugin.shardServices());
        }
        return services;
    }

    Settings additionalSettings() {
        Settings.Builder builder = Settings.settingsBuilder();
        for (Plugin plugin : plugins) {
            builder.put(plugin.additionalSettings());
        }
        return builder.build();
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
                            logger.warn("Plugin {}, failed to invoke custom onModule method", e, plugin.name());
                        }
                    }
                }
            }
        }
    }

    private void checkJarHell(URL url) throws Exception {
        final List<URL> loadedJars = new ArrayList<>(Arrays.asList(JarHell.parseClassPath()));
        loadedJars.addAll(jarsToLoad);
        loadedJars.add(url);
        JarHell.checkJarHell(loadedJars.toArray(new URL[0]));
    }
}
