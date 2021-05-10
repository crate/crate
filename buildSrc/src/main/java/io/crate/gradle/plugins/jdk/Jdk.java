/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
    private static final List<String> ALLOWED_OS = List.of("linux", "windows", "mac");
    private static final List<String> ALLOWED_ARCH = List.of("x64", "aarch64");
    private static final Pattern VERSION_PATTERN = Pattern.compile(
        "(\\d+)(\\.\\d+\\.\\d+)?\\+(\\d+(?:\\.\\d+)?)(@([a-f0-9]{32}))?");

    private final String name;
    private final Configuration configuration;

    private final Property<String> vendor;
    private final Property<String> version;
    private final Property<String> arch;
    private final Property<String> os;
    private String baseVersion;
    private String major;
    private String build;
    private String hash;

    Jdk(String name, Configuration configuration, ObjectFactory objectFactory) {
        this.name = name;
        this.configuration = configuration;
        this.vendor = objectFactory.property(String.class);
        this.version = objectFactory.property(String.class);
        this.arch = objectFactory.property(String.class);
        this.os = objectFactory.property(String.class);
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
        var minor = versionMatcher.group(2);
        baseVersion = minor == null
            ? versionMatcher.group(1)
            : versionMatcher.group(1) + minor;
        major = versionMatcher.group(1);
        build = versionMatcher.group(3);
        hash = versionMatcher.group(5);
        this.version.set(version);
    }

    public String platform() {
        return arch() + "_" + os();
    }

    public String arch() {
        return arch.get();
    }

    public void setArch(String arch) {
        if (!ALLOWED_ARCH.contains(arch)) {
            throw new IllegalArgumentException(
                "unknown architecture [" + arch + "] for jdk [" + name + "], " +
                "must be one of " + ALLOWED_ARCH
            );
        }
        this.arch.set(arch);
    }

    public String os() {
        return os.get();
    }

    public void setOs(String os) {
        if (!ALLOWED_OS.contains(os)) {
            throw new IllegalArgumentException(
                "unknown OS [" + os + "] for jdk [" + name + "], " +
                "must be one of " + ALLOWED_OS
            );
        }
        this.os.set(os);
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
        return new File(path() + ("mac".equals(os()) ? "/Contents/Home" : ""));
    }

    @SuppressWarnings("UnstableApiUsage")
    void finalizeValues() {
        if (!version.isPresent()) {
            throw new IllegalArgumentException("version is not specified for jdk [" + name + "]");
        }
        if (!os.isPresent()) {
            throw new IllegalArgumentException("OS is not specified for jdk [" + name + "]");
        }
        if (!arch.isPresent()) {
            throw new IllegalArgumentException("architecture is not specified for jdk [" + name + "]");
        }
        if (!vendor.isPresent()) {
            throw new IllegalArgumentException("vendor is not specified for jdk [" + name + "]");
        }
        version.finalizeValue();
        os.finalizeValue();
        arch.finalizeValue();
        vendor.finalizeValue();
    }

    @Override
    public Iterator<File> iterator() {
        return configuration.iterator();
    }
}
