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

import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class JdkTest {

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private static Project rootProject;

    @BeforeClass
    public static void setupRoot() {
        rootProject = ProjectBuilder.builder().build();
    }

    @Test
    public void testMissingArch() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
            "architecture is not specified for jdk [testjdk]");
        createJdk(createProject(),
            "testjdk",
            null,
            "11.0.2+33",
            "linux",
            null
        );
    }

    @Test
    public void testUnknownArch() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
            "unknown architecture [x86] for jdk [testjdk], must be one of [x64, aarch64]");
        createJdk(createProject(),
            "testjdk",
            "adoptopenjdk",
            "11.0.2+33",
            "windows",
            "x86"
        );
    }

    @Test
    public void testMissingVendor() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
            "vendor is not specified for jdk [testjdk]");
        createJdk(createProject(),
            "testjdk",
            null,
            "11.0.2+33",
            "linux",
            "x64"
        );
    }

    @Test
    public void testUnknownVendor() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
            "unknown vendor [unknown] for jdk [testjdk], must be one of [adoptopenjdk]");
        createJdk(createProject(),
            "testjdk",
            "unknown",
            "11.0.2+33",
            "linux",
            "x64"
        );
    }

    @Test
    public void testMissingVersion() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("version is not specified for jdk [testjdk]");
        createJdk(createProject(),
            "testjdk",
            "adoptopenjdk",
            null,
            "linux",
            "x64"
        );
    }

    @Test
    public void testBadVersionFormat() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("malformed version [badversion] for jdk [testjdk]");
        createJdk(createProject(),
            "testjdk",
            "adoptopenjdk",
            "badversion",
            "linux",
            "x64"
        );
    }

    @Test
    public void testMissingOS() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("OS is not specified for jdk [testjdk]");
        createJdk(createProject(),
            "testjdk",
            "adoptopenjdk",
            "11.0.2+33",
            null,
            "x64"
        );
    }

    @Test
    public void testUnknownOS() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
            "unknown OS [unknown] for jdk [testjdk], " +
            "must be one of [linux, windows, mac]");
        createJdk(createProject(),
            "testjdk",
            "adoptopenjdk",
            "11.0.2+33",
            "unknown",
            "x64"
        );
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
