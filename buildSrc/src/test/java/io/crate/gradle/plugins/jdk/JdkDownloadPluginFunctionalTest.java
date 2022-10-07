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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.head;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.GradleRunner;
import org.junit.Test;

import com.github.tomakehurst.wiremock.WireMockServer;

/**
 * Note: the downloading of the JDK bundles are not covered by the test,
 * such as it is done by ivy repositories. We skip this step by creating
 * the fake repository with fake JDK binaries.
 */
public class JdkDownloadPluginFunctionalTest {

    private static final Pattern JDK_HOME_LOG = Pattern.compile("JDK HOME: (.*)");

    // The JDK vendor and version is hardcoded. It depends on
    // on the fake JDK artifacts from the test resources.
    public static final String VENDOR = "adoptopenjdk";
    public static final String VERSION = "13.0.2+8";

    @Test
    public void testAarch64LinuxJDKExtraction() {
        assertExtraction("getAarch64LinuxJdk", "bin/java", "linux", "aarch64", VENDOR, VERSION);
    }

    @Test
    public void testX64LinuxJDKExtraction() {
        assertExtraction("getX64LinuxJdk", "bin/java", "linux", "x64", VENDOR, VERSION);
    }

    @Test
    public void testMacJDKExtraction() {
        assertExtraction("getMacJdk", "Contents/Home/bin/java", "mac", "x64", VENDOR, VERSION);
    }

    @Test
    public void testWindowsJDKExtraction() {
        assertExtraction("getWindowsJdk", "bin/java", "windows", "x64", VENDOR, VERSION);
    }

    private static void assertExtraction(String task,
                                         String javaBin,
                                         String os,
                                         String arch,
                                         String vendor,
                                         String version) {
        runBuild(task, result -> {
            Matcher matcher = JDK_HOME_LOG.matcher(result.getOutput());
            assertThat(matcher.find())
                .as("could not find jdk home in: " + result.getOutput())
                .isTrue();
            String jdkHome = matcher.group(1);
            Path javaPath = Paths.get(jdkHome, javaBin);
            assertThat(Files.exists(javaPath))
                .as(javaPath.toString())
                .isTrue();
        }, os, arch, vendor, version);
    }

    private static void runBuild(String task,
                                 Consumer<BuildResult> assertions,
                                 String os,
                                 String arch,
                                 String vendor,
                                 String version) {
        var testKitDirPath = Paths.get(
            System.getProperty("user.dir"),
            "build",
            System.getProperty("user.name")
        );

        WireMockServer wireMock = new WireMockServer(0);
        try {
            wireMock.stubFor(
                head(
                    urlEqualTo(urlPath(vendor, version, os, arch))
                ).willReturn(aResponse().withStatus(200))
            );
            wireMock.stubFor(
                get(
                    urlEqualTo(urlPath(vendor, version, os, arch))
                ).willReturn(
                    aResponse()
                        .withStatus(200)
                        .withBody(filebytes(vendor, version, os, arch))
                )
            );
            wireMock.start();

            GradleRunner runner = GradleRunner
                .create()
                .withDebug(true)
                .withProjectDir(getTestKitProjectDir("jdk-download"))
                .withTestKitDir(testKitDirPath.toFile())
                .withArguments(
                    task,
                    "-Dtests.jdk_vendor=" + vendor,
                    "-Dtests.jdk_version=" + version,
                    "-Dtests.jdk_repo=" + wireMock.baseUrl(),
                    "-i")
                .withPluginClasspath();

            BuildResult result = runner.build();
            assertions.accept(result);
        } catch (IOException e) {
            fail("cannot get artifacts from resources: " + e.getLocalizedMessage());
        } finally {
            wireMock.stop();
        }
    }

    private static String urlPath(String vendor,
                                  String version,
                                  String os,
                                  String arch) {
        if (vendor.equals("adoptopenjdk")) {
            return "/jdk-" +
                   version + "/" +
                   os + "/" +
                   arch + "/" +
                   "jdk/hotspot/normal/adoptopenjdk";
        } else {
            throw new IllegalArgumentException(vendor + " is not supported by the test setup");
        }
    }

    private static byte[] filebytes(String vendor,
                                    String version,
                                    String os,
                                    String arch) throws IOException {
        var extension = os.equals("windows") ? "zip" : "tar.gz";
        if (vendor.equals("adoptopenjdk")) {
            try (var inputStream =
                     JdkDownloadPluginFunctionalTest.class.getResourceAsStream(
                        "/fake_artifacts/" +
                        version + "/" +
                        arch + "_" +
                        os + "." +
                        extension)) {
                return inputStream.readAllBytes();
            }
        } else {
            throw new IllegalArgumentException(vendor + " is not supported by the test setup");
        }
    }

    private static File getTestKitProjectDir(String name) {
        File root = new File("src/testKit/");
        if (!root.exists()) {
            throw new RuntimeException("Could not find resources dir for integration tests");
        }
        return new File(root, name).getAbsoluteFile();
    }
}
