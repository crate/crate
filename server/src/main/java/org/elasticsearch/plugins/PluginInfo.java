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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * An in-memory representation of the plugin descriptor.
 */
public class PluginInfo {

    public static final String ES_PLUGIN_PROPERTIES = "plugin-descriptor.properties";

    private final String name;
    private final String description;
    private final String classname;

    /**
     * Construct plugin info.
     *
     * @param name                  the name of the plugin
     * @param description           a description of the plugin
     * @param classname             the entry point to the plugin
     */
    public PluginInfo(String name, String description, String classname) {
        this.name = name;
        this.description = description;
        this.classname = classname;
    }

    /**
     * Reads the plugin descriptor file.
     *
     * @param path           the path to the root directory for the plugin
     * @return the plugin info
     * @throws IOException if an I/O exception occurred reading the plugin descriptor
     */
    public static PluginInfo readFromProperties(final Path path) throws IOException {
        final Path descriptor = path.resolve(ES_PLUGIN_PROPERTIES);
        final Properties props = new Properties();
        try (InputStream stream = Files.newInputStream(descriptor)) {
            props.load(stream);
        }

        final String name = (String) props.remove("name");
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException(
                    "property [name] is missing in [" + descriptor + "]");
        }
        final String description = (String) props.remove("description");
        if (description == null) {
            throw new IllegalArgumentException(
                    "property [description] is missing for plugin [" + name + "]");
        }
        final String classname = (String) props.remove("classname");
        if (classname == null) {
            throw new IllegalArgumentException(
                    "property [classname] is missing for plugin [" + name + "]");
        }

        if (props.isEmpty() == false) {
            throw new IllegalArgumentException("Unknown properties in plugin descriptor: " + props.keySet());
        }
        return new PluginInfo(name, description, classname);
    }

    /**
     * The name of the plugin.
     *
     * @return the plugin name
     */
    public String getName() {
        return name;
    }

    /**
     * The description of the plugin.
     *
     * @return the plugin description
     */
    public String getDescription() {
        return description;
    }

    /**
     * The entry point to the plugin.
     *
     * @return the entry point to the plugin
     */
    public String getClassname() {
        return classname;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PluginInfo that = (PluginInfo) o;

        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return toString("");
    }

    public String toString(String prefix) {
        final StringBuilder information = new StringBuilder()
            .append(prefix).append("- Plugin information:\n")
            .append(prefix).append("Name: ").append(name).append("\n")
            .append(prefix).append("Description: ").append(description).append("\n")
            .append(prefix).append(" * Classname: ").append(classname);
        return information.toString();
    }
}
