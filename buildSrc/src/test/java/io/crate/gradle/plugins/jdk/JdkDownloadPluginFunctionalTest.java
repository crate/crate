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

import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.GradleRunner;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Note: the downloading of the JDK bundles are not covered by the test,
 * such as it is done by ivy repositories. We skip this step by creating
 * the fake repository with fake JDK binaries.
 */
public class JdkDownloadPluginFunctionalTest {

    private static final Pattern JDK_HOME_LOG = Pattern.compile("JDK HOME: (.*)");

    // The JDK vendor and version is hardcoded. It depends on
    // on the defined fake ivy repository in the testKit directory.
    public static final String VENDOR = "adoptopenjdk";
    public static final String VERSION = "13.0.2+8";

    @Test
    public void testAarch64LinuxJDKExtraction() {
        assertExtraction("getAarch64LinuxJdk", "bin/java", VENDOR, VERSION);
    }

    @Test
    public void testX64LinuxJDKExtraction() {
        assertExtraction("getX64LinuxJdk", "bin/java", VENDOR, VERSION);
    }

    @Test
    public void testMacJDKExtraction() {
        assertExtraction("getMacJdk", "Contents/Home/bin/java", VENDOR, VERSION);
    }

    @Test
    public void testWindowsJDKExtraction() {
        assertExtraction("getWindowsJdk", "bin/java", VENDOR, VERSION);
    }

    private static void assertExtraction(String task,
                                         String javaBin,
                                         String vendor,
                                         String version) {
        runBuild(task, result -> {
            Matcher matcher = JDK_HOME_LOG.matcher(result.getOutput());
            assertThat("could not find jdk home in: " + result.getOutput(), matcher.find(), is(true));
            String jdkHome = matcher.group(1);
            Path javaPath = Paths.get(jdkHome, javaBin);
            assertThat(javaPath.toString(), Files.exists(javaPath), is(true));
        }, vendor, version);
    }

    private static void runBuild(String task,
                                 Consumer<BuildResult> assertions,
                                 String vendor,
                                 String version) {
        var testKitDirPath = Paths.get(
            System.getProperty("user.dir"),
            "build",
            System.getProperty("user.name")
        );
        GradleRunner runner = GradleRunner.create()
            .withDebug(true)
            .withProjectDir(getTestKitProjectDir("jdk-download"))
            .withTestKitDir(testKitDirPath.toFile())
            .withArguments(
                task,
                "-Dtests.jdk_vendor=" + vendor,
                "-Dtests.jdk_version=" + version,
                "-i"
            )
            .withPluginClasspath();

        BuildResult result = runner.build();
        assertions.accept(result);
    }

    private static File getTestKitProjectDir(String name) {
        File root = new File("src/testKit/");
        if (!root.exists()) {
            throw new RuntimeException("Could not find resources dir for integration tests");
        }
        return new File(root, name).getAbsoluteFile();
    }
}
