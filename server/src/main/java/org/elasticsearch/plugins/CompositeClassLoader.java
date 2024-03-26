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

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

public class CompositeClassLoader extends ClassLoader {

    private final List<ClassLoader> loaders;

    public CompositeClassLoader(ClassLoader parent, List<ClassLoader> loaders) {
        super(parent);
        this.loaders = Collections.unmodifiableList(loaders);
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        for (ClassLoader loader : loaders) {
            try {
                return loader.loadClass(name);
            } catch (ClassNotFoundException e) {
                // continue
            }
        }
        throw new ClassNotFoundException(name);
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        List<URL> urls = new ArrayList<>();
        super.getResources(name).asIterator().forEachRemaining(urls::add);
        for (var loader : loaders) {
            Enumeration<URL> resources = loader.getResources(name);
            resources.asIterator().forEachRemaining(urls::add);
        }
        return Collections.enumeration(urls);
    }
}
