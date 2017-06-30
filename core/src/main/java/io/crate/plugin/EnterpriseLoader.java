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


import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Objects;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

/**
 * Utility class to load classes dynamically using a serviceLoader.
 * (Classes are discovered using META-INF/services files)
 */
public final class EnterpriseLoader {

    private final static Logger LOGGER = Loggers.getLogger(EnterpriseLoader.class);

    /**
     * Load a single instance of {@code clazz} from contextClassLoader or from any jar within {@code jarDirectory}.
     * <br />
     * {@code jarDirectory} will be traversed up to 3 levels.
     */
    @Nullable
    public static <T> T loadSingle(Path jarDirectory, Class<T> clazz) throws IOException {
        ClassLoader parentCL = Thread.currentThread().getContextClassLoader();
        int maxDepth = 3;
        URL[] urls;
        try {
            urls = Files.walk(jarDirectory, maxDepth)
                .map(EnterpriseLoader::toURLOrNull)
                .filter(Objects::nonNull)
                .filter(url -> url.getFile().endsWith(".jar"))
                .toArray(URL[]::new);
        } catch (NoSuchFileException ignored) {
            // this should only happen in tests where the folder that's created in the distribution gradle task doesn't exist
            LOGGER.debug("Couldn't find jarDirectory \"" + ignored.getFile() + "\" to load: " + clazz.getName());
            urls = new URL[0];
        }
        URLClassLoader classLoader = new URLClassLoader(urls, parentCL);
        return loadSingle(clazz, classLoader);
    }

    @Nullable
    public static <T> T loadSingle(Class<T> clazz, ClassLoader classLoader) {
        Iterator<T> it = ServiceLoader.load(clazz, classLoader).iterator();
        T instance = null;
        while (it.hasNext()) {
            if (instance != null) {
                throw new ServiceConfigurationError(clazz.getSimpleName() + " found twice");
            }
            instance = it.next();
        }
        return instance;
    }

    @Nullable
    private static URL toURLOrNull(Path p) {
        try {
            return p.toUri().toURL();
        } catch (MalformedURLException e) {
            return null;
        }
    }

}
