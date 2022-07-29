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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.BeforeClass;
import org.junit.Test;

public class JdkTest {

    private static Project rootProject;

    @BeforeClass
    public static void setupRoot() {
        rootProject = ProjectBuilder.builder().build();
    }

    @Test
    public void testMissingArch() {
        assertThatThrownBy(() ->
           createJdk(createProject(),
                     "testjdk",
                     null,
                     "11.0.2+33",
                     "linux",
                     null
           ))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("architecture is not specified for jdk [testjdk]");
    }

    @Test
    public void testUnknownArch() {
        assertThatThrownBy(() ->
           createJdk(createProject(),
                     "testjdk",
                     "adoptopenjdk",
                     "11.0.2+33",
                     "windows",
                     "x86"
           ))
           .isExactlyInstanceOf(IllegalArgumentException.class)
           .hasMessage("unknown architecture [x86] for jdk [testjdk], must be one of [x64, aarch64]");

    }

    @Test
    public void testMissingVendor() {
        assertThatThrownBy(() ->
           createJdk(createProject(),
                     "testjdk",
                     null,
                     "11.0.2+33",
                     "linux",
                     "x64"
           ))
           .isExactlyInstanceOf(IllegalArgumentException.class)
           .hasMessage("vendor is not specified for jdk [testjdk]");
    }

    @Test
    public void testUnknownVendor() {
        assertThatThrownBy(() ->
           createJdk(createProject(),
                     "testjdk",
                     "unknown",
                     "11.0.2+33",
                     "linux",
                     "x64"
           ))
           .isExactlyInstanceOf(IllegalArgumentException.class)
           .hasMessage("unknown vendor [unknown] for jdk [testjdk], must be one of [adoptopenjdk, adoptium]");
    }

    @Test
    public void testMissingVersion() {
        assertThatThrownBy(() ->
           createJdk(createProject(),
                     "testjdk",
                     "adoptopenjdk",
                     null,
                     "linux",
                     "x64"
           ))
           .isExactlyInstanceOf(IllegalArgumentException.class)
           .hasMessage("version is not specified for jdk [testjdk]");

    }

    @Test
    public void testBadVersionFormat() {
        assertThatThrownBy(() ->
           createJdk(createProject(),
                     "testjdk",
                     "adoptopenjdk",
                     "badversion",
                     "linux",
                     "x64"
           ))
           .isExactlyInstanceOf(IllegalArgumentException.class)
           .hasMessage("malformed version [badversion] for jdk [testjdk]");
    }

    @Test
    public void testMissingOS() {
        assertThatThrownBy(() ->
           createJdk(createProject(),
                     "testjdk",
                     "adoptopenjdk",
                     "11.0.2+33",
                     null,
                     "x64"
           ))
           .isExactlyInstanceOf(IllegalArgumentException.class)
           .hasMessage("OS is not specified for jdk [testjdk]");
    }

    @Test
    public void testUnknownOS() {
        assertThatThrownBy(() ->
           createJdk(createProject(),
                     "testjdk",
                     "adoptopenjdk",
                     "11.0.2+33",
                     "unknown",
                     "x64"
           ))
           .isExactlyInstanceOf(IllegalArgumentException.class)
           .hasMessage("unknown OS [unknown] for jdk [testjdk], must be one of [linux, windows, mac]");
    }

    private void createJdk(Project project,
                           String name,
                           String vendor,
                           String version,
                           String os,
                           String arch) {
        //noinspection unchecked
        var jdks = (NamedDomainObjectContainer<Jdk>) project.getExtensions().getByName("jdks");
        jdks.create(name, jdk -> {
            if (vendor != null) {
                jdk.setVendor(vendor);
            }
            if (version != null) {
                jdk.setVersion(version);
            }
            if (os != null) {
                jdk.setOs(os);
            }
            if (arch != null) {
                jdk.setArch(arch);
            }
        }).finalizeValues();
    }

    private Project createProject() {
        Project project = ProjectBuilder.builder().withParent(rootProject).build();
        project.getPlugins().apply("jdk-download");
        return project;
    }
}
