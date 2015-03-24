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

package io.crate.client;

import com.google.common.base.Joiner;
import io.crate.core.StringUtils;

import java.util.regex.Pattern;

/**
 * Capable of loading classes that have been relocated due to shading of the client jar
 * This is necessary because exceptions are serialized using java serialization
 * containing the fully qualified class names from the crate server :/
 *
 */
public class CrateClientClassLoader extends ClassLoader {

    private static final String SHADE_PACKAGE_PREFIX = "io.crate.shade";
    private static final Pattern SHADED_PREFIXES_PATTERN = Pattern.compile(
            "(^" + Joiner.on(")|(^").join(
                    // gradle shadow plugin does dumb regex replace at word boundary
                    // across the whole source, use substring to avoid that
                    "Oorg.elasticsearch".substring(1),
                    "Oorg.apache.lucene".substring(1),
                    "Oorg.joda".substring(1),
                    "Oorg.tartarus.snowball".substring(1),
                    "Ocom.carrotsearch.hppc".substring(1),
                    "Ocom.fasterxml.jackson".substring(1),
                    "Ocom.google".substring(1),
                    "Ocom.ning.compress".substring(1),
                    "Oorg.jboss.netty".substring(1),
                    "Oorg.apache.commons".substring(1),
                    "Ojsr166".substring(1)
            ) + ")"
    );

    public CrateClientClassLoader(ClassLoader parent) {
        super(parent);
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        if (SHADED_PREFIXES_PATTERN.matcher(name).find()) {
            // proactively try to load dependency classes with shade prefix
            // so even if those elasticsearch classes are present on the classpath
            // we first try to load our versions
            try {
                return super.loadClass(StringUtils.PATH_JOINER.join(SHADE_PACKAGE_PREFIX, name));
            } catch (ClassNotFoundException e) {
                // proceed with normal name
            }
        }
        return super.loadClass(name);
    }
}
