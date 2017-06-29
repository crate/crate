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
import io.crate.Plugin;
import org.apache.logging.log4j.Logger;
import org.apache.xbean.finder.ResourceFinder;
import org.elasticsearch.bootstrap.JarHell;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.PluginInfo;

import java.io.File;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.elasticsearch.common.io.FileSystemUtils.isAccessibleDirectory;

public class PluginLoader {

    static final Setting<String> SETTING_CRATE_PLUGINS_PATH = Setting.simpleString(
        "path.crate_plugins", Setting.Property.NodeScope);

    private static final String RESOURCE_PATH = "META-INF/services/";
    private static final String ENTERPRISE_FOLDER_NAME = "enterprise";

    private final Settings settings;
    private final Logger logger;

    @VisibleForTesting
    final List<Plugin> plugins;
    private final Path pluginsPath;
    private final List<URL> jarsToLoad = new ArrayList<>();

    PluginLoader(Settings settings) {
        this.settings = settings;

        String pluginFolder = SETTING_CRATE_PLUGINS_PATH.get(settings);
        if (pluginFolder.isEmpty()) {
            pluginsPath = PathUtils.get(Strings.cleanPath(settings.get("path.home"))).resolve("plugins");
        } else {
            pluginsPath = PathUtils.get(Strings.cleanPath(pluginFolder));
        }
        logger = Loggers.getLogger(getClass().getPackage().getName(), settings);

        Collection<Class<? extends Plugin>> implementations = findImplementations();

        ImmutableList.Builder<Plugin> builder = ImmutableList.builder();
        for (Class<? extends Plugin> pluginClass : implementations) {
            try {
                builder.add(loadPlugin(pluginClass));
            } catch (Throwable t) {
                logger.error("error loading plugin:  " + pluginClass.getSimpleName(), t);
            }
        }
        plugins = builder.build();

        if (logger.isInfoEnabled()) {
            logger.info("plugins loaded: {} ", plugins.stream().map(Plugin::name).collect(Collectors.toList()));
        }
    }

    private Collection<Class<? extends Plugin>> findImplementations() {
        if (!isAccessibleDirectory(pluginsPath, logger)) {
            return Collections.emptyList();
        }

        final File[] plugins = pluginsPath.toFile()
            .listFiles(file -> !file.getName().equals(ENTERPRISE_FOLDER_NAME));

        if (plugins == null) {
            return Collections.emptyList();
        }

        final File[] allPlugins;
        File enterprisePluginDir = new File(pluginsPath.toFile(), ENTERPRISE_FOLDER_NAME);
        File[] enterpriseFiles = enterprisePluginDir.listFiles();
        if (enterpriseFiles != null) {
            allPlugins = Arrays.copyOf(plugins, plugins.length + enterpriseFiles.length);
            System.arraycopy(enterpriseFiles, 0, allPlugins, plugins.length, enterpriseFiles.length);
        } else {
            allPlugins = plugins;
        }

        Collection<Class<? extends Plugin>> allImplementations = new ArrayList<>();
        for (File plugin : allPlugins) {
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

    Collection<Module> createGuiceModules() {
        List<Module> modules = new ArrayList<>();
        for (Plugin plugin : plugins) {
            modules.addAll(plugin.createGuiceModules());
        }
        return modules;
    }

    Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        List<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        for (Plugin plugin : plugins) {
            services.addAll(plugin.getGuiceServiceClasses());
        }
        return services;
    }

    Settings additionalSettings() {
        Settings.Builder builder = Settings.builder();
        for (Plugin plugin : plugins) {
            builder.put(plugin.additionalSettings());
        }
        return builder.build();
    }

    List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>();
        for (Plugin plugin : plugins) {
            settings.addAll(plugin.getSettings());
        }
        return settings;
    }


    private void checkJarHell(URL url) throws Exception {
        final List<URL> loadedJars = new ArrayList<>(Arrays.asList(JarHell.parseClassPath()));
        loadedJars.addAll(jarsToLoad);
        loadedJars.add(url);
        JarHell.checkJarHell(loadedJars.toArray(new URL[0]));
    }
}
