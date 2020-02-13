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

package io.crate.gradle.plugins.jdk;

import org.gradle.api.Buildable;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.TaskDependency;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class Jdk implements Buildable, Iterable<File> {

    private static final List<String> ALLOWED_VENDORS = List.of("adoptopenjdk");
    private static final List<String> ALLOWED_PLATFORMS = List.of("linux", "windows", "mac");
    private static final Pattern VERSION_PATTERN = Pattern.compile(
        "(\\d+)(\\.\\d+\\.\\d+)?\\+(\\d+(?:\\.\\d+)?)(@([a-f0-9]{32}))?");

    private final String name;
    private final Configuration configuration;

    private final Property<String> vendor;
    private final Property<String> version;
    private final Property<String> platform;
    private String baseVersion;
    private String major;
    private String build;
    private String hash;

    Jdk(String name, Configuration configuration, ObjectFactory objectFactory) {
        this.name = name;
        this.configuration = configuration;
        this.vendor = objectFactory.property(String.class);
        this.version = objectFactory.property(String.class);
        this.platform = objectFactory.property(String.class);
    }

    public String name() {
        return name;
    }

    public String vendor() {
        return vendor.get();
    }

    public void setVendor(final String vendor) {
        if (!ALLOWED_VENDORS.contains(vendor)) {
            throw new IllegalArgumentException(
                "unknown vendor [" + vendor + "] for jdk [" + name + "], " +
                "must be one of " + ALLOWED_VENDORS);
        }
        this.vendor.set(vendor);
    }

    public String version() {
        return version.get();
    }

    public void setVersion(String version) {
        var versionMatcher = VERSION_PATTERN.matcher(version);
        if (!versionMatcher.matches()) {
            throw new IllegalArgumentException("malformed version [" + version + "] for jdk [" + name + "]");
        }
        baseVersion = versionMatcher.group(1) + versionMatcher.group(2);
        major = versionMatcher.group(1);
        build = versionMatcher.group(3);
        hash = versionMatcher.group(5);
        this.version.set(version);
    }

    public String platform() {
        return platform.get();
    }

    public void setPlatform(String platform) {
        if (!ALLOWED_PLATFORMS.contains(platform)) {
            throw new IllegalArgumentException(
                "unknown platform [" + platform + "] for jdk [" + name + "], " +
                "must be one of " + ALLOWED_PLATFORMS
            );
        }
        this.platform.set(platform);
    }

    public String baseVersion() {
        return baseVersion;
    }

    public String major() {
        return major;
    }

    public String build() {
        return build;
    }

    public String hash() {
        return hash;
    }

    public String path() {
        return configuration.getSingleFile().toString();
    }

    public Configuration configuration() {
        return configuration;
    }

    @Override
    public String toString() {
        return path();
    }

    @Override
    public TaskDependency getBuildDependencies() {
        return configuration.getBuildDependencies();
    }

    public Object getBinJavaPath() {
        return new Object() {
            @Override
            public String toString() {
                return getJavaHome().getAbsolutePath() + "/bin/java";
            }
        };
    }

    public File getJavaHome() {
        return new File(path() + ("mac".equals(platform()) ? "/Contents/Home" : ""));
    }

    @SuppressWarnings("UnstableApiUsage")
    void finalizeValues() {
        if (!version.isPresent()) {
            throw new IllegalArgumentException("version not specified for jdk [" + name + "]");
        }
        if (!platform.isPresent()) {
            throw new IllegalArgumentException("platform not specified for jdk [" + name + "]");
        }
        if (!vendor.isPresent()) {
            throw new IllegalArgumentException("vendor not specified for jdk [" + name + "]");
        }
        version.finalizeValue();
        platform.finalizeValue();
        vendor.finalizeValue();
    }

    @Override
    public Iterator<File> iterator() {
        return configuration.iterator();
    }
}
