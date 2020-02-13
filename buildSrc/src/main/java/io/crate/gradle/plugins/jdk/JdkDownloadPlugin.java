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

import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.dsl.RepositoryHandler;
import org.gradle.api.artifacts.repositories.IvyArtifactRepository;
import org.gradle.api.file.CopySpec;
import org.gradle.api.file.Directory;
import org.gradle.api.file.FileTree;
import org.gradle.api.file.RelativePath;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.TaskProvider;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

/**
 * The plugin exposes the `jdks` extension for configuring
 * a set of JDK bundles. For instance:
 *
 * <p>
 * apply plugin: 'jdk-download'
 *
 * jdks {
 * ...
 *  linux {
 *      platform="linux"
 *      vendor="adoptopenjdk"
 *      version="13.0.2+8"
 *  }
 *  mac {
 *      platform="linux"
 *      vendor="adoptopenjdk"
 *      version="13.0.2+8"
 *  }
 *  ...
 * }
 *
 * <p>
 * Regardless whether the plugin applied on the sub or root project, on the root project
 * it will create two configurations and one task per configured JDK bundle:
 * <p>
 * - The download configuration that is used to resolve
 * the configured JDK bundle artifact.
 * - The extract configuration that is used to extract
 * the artifact.
 * - The extract task that is used to untar or unzip the bundle.
 * <p>
 * For sub-projects the plugin creates the configuration that depends
 * on the extract configuration of the root project. The configuration name
 * is defined by the extension name plus bunle name, e.g. jdks.linux
 * <p>
 * <p>
 * Usage:
 * <p>
 * task resolveAndPrintLinuxJdkJavaHomePath {
 *      dependsOn project.jdks.linux
 *      doLast {
 *          println project.jdks.linux.getJavaHome()
 *      }
 * }
 *
 * task copyLinuxJdkBundle(type: Copy) {
 *      from project.jdks.linux
 *      into "${project.buildDir}/dist/..."
 * }
 */
public class JdkDownloadPlugin implements Plugin<Project> {

    private static final String REPO_NAME_PREFIX = "jdk_repo_";
    private static final String EXTENSION_NAME = "jdks";

    @Override
    public void apply(Project project) {
        NamedDomainObjectContainer<Jdk> jdksContainer = project.container(
            Jdk.class,
            name -> new Jdk(name, project.getConfigurations().create("jdk_" + name), project.getObjects())
        );
        project.getExtensions().add(EXTENSION_NAME, jdksContainer);

        project.afterEvaluate(p ->
            jdksContainer.all(jdk -> {
                jdk.finalizeValues();
                // depend on the jdk directory "artifact" from the root project
                var dependencies = project.getDependencies();
                Map<String, Object> depConfig = Map.of(
                    "path", ":",
                    "configuration", configName(
                        "extract",
                        jdk.vendor(),
                        jdk.version(),
                        jdk.platform()));
                dependencies.add(
                    jdk.configuration().getName(),
                    dependencies.project(depConfig));

                setupRootJdkDownload(project.getRootProject(), jdk);
            })
        );
    }

    private static void setupRootJdkDownload(Project rootProject, Jdk jdk) {
        var extractTaskName =
            "extract_" + jdk.platform().toLowerCase() + "_jdk_" + jdk.vendor() + "_" + jdk.version();

        // Skip setup if we've already configured a JDK for
        // this platform, vendor and version
        if (rootProject.getTasks().findByPath(extractTaskName) == null) {
            var downloadConfiguration = maybeCreateRepositoryAndDownloadConfiguration(rootProject, jdk);
            var extractPathProvider = rootProject.getLayout()
                .getBuildDirectory()
                .dir("jdks/" + jdk.vendor() + "-" + jdk.baseVersion() + "_" + jdk.platform());
            TaskProvider<?> extractTask = createExtractTask(
                extractTaskName,
                rootProject,
                jdk.platform(),
                extractPathProvider,
                downloadConfiguration::getSingleFile
            );

            // Declare a configuration for the extracted JDK archive
            String artifactConfigName = configName("extract", jdk.vendor(), jdk.version(), jdk.platform());
            rootProject.getConfigurations().maybeCreate(artifactConfigName);
            rootProject.getArtifacts().add(
                artifactConfigName,
                extractPathProvider,
                artifact -> artifact.builtBy(extractTask));
        }
    }

    private static Configuration maybeCreateRepositoryAndDownloadConfiguration(Project rootProject, Jdk jdk) {
        RepositoryHandler repositories = rootProject.getRepositories();
        String repoUrl;
        String artifactPattern;
        if (jdk.vendor().equals("adoptopenjdk")) {
            repoUrl = "https://cdn.crate.io/downloads/openjdk/";
            artifactPattern = String.format(
                Locale.ENGLISH,
                "OpenJDK%sU-jdk_x64_[module]_hotspot_[revision]_%s.[ext]",
                jdk.major(),
                jdk.build()
            );
        } else {
            throw new GradleException("Unknown JDK vendor [" + jdk.vendor() + "]");
        }

        // Define the appropriate repository for the given JDK vendor and version
        var repoName = REPO_NAME_PREFIX + jdk.vendor() + "_" + jdk.version();
        if (rootProject.getRepositories().findByName(repoName) == null) {
            repositories.ivy(ivyRepo -> {
                ivyRepo.setName(repoName);
                ivyRepo.setUrl(repoUrl);
                ivyRepo.metadataSources(IvyArtifactRepository.MetadataSources::artifact);
                ivyRepo.patternLayout(layout -> layout.artifact(artifactPattern));
                ivyRepo.content(content -> content.includeGroup(jdk.vendor()));
            });
        }

        // Declare a configuration and dependency from which to download the remote JDK
        var downloadConfigName = configName(jdk.vendor(), jdk.version(), jdk.platform());
        var downloadConfiguration = rootProject.getConfigurations().maybeCreate(downloadConfigName);
        rootProject.getDependencies().add(downloadConfigName, dependencyNotation(jdk));
        return downloadConfiguration;
    }

    private static TaskProvider<?> createExtractTask(
        String taskName,
        Project rootProject,
        String platform,
        Provider<Directory> extractPath,
        Supplier<File> jdkBundle) {
        if (platform.equals("windows")) {
            Action<CopySpec> removeRootDir = copy -> {
                // remove extra unnecessary directory levels
                copy.eachFile(details -> {
                    Path newPathSegments = trimArchiveExtractPath(details.getRelativePath().getPathString());
                    String[] segments = StreamSupport.stream(newPathSegments.spliterator(), false)
                        .map(Path::toString)
                        .toArray(String[]::new);
                    details.setRelativePath(new RelativePath(true, segments));
                });
                copy.setIncludeEmptyDirs(false);
            };

            return rootProject.getTasks().register(taskName, Copy.class, copyTask -> {
                copyTask.doFirst(t -> rootProject.delete(extractPath));
                copyTask.into(extractPath);
                Callable<FileTree> fileGetter = () -> rootProject.zipTree(jdkBundle.get());
                copyTask.from(fileGetter, removeRootDir);
            });
        } else {
            /*
             * Gradle TarFileTree does not resolve symlinks, so we have to manually
             * extract and preserve the symlinks. cf. https://github.com/gradle/gradle/issues/3982
             * and https://discuss.gradle.org/t/tar-and-untar-losing-symbolic-links/2039
             */
            return rootProject.getTasks().register(taskName, SymbolicLinkPreservingUntarTask.class, task -> {
                task.getTarFile().fileProvider(rootProject.provider(jdkBundle::get));
                task.getExtractPath().set(extractPath);
                task.setTransform(JdkDownloadPlugin::trimArchiveExtractPath);
            });
        }
    }

    /*
     * We want to remove up to the and including the jdk-.* relative paths.
     * That is a JDK archive is structured as:
     *   jdk-12.0.1/
     *   jdk-12.0.1/Contents
     *   ...
     *
     * and we want to remove the leading jdk-12.0.1. Note however that there
     * could also be a leading ./ as in
     *   ./
     *   ./jdk-12.0.1/
     *   ./jdk-12.0.1/Contents
     *
     * so we account for this and search the path components until we find the
     * jdk-12.0.1, and strip the leading components.
     */
    private static Path trimArchiveExtractPath(String relativePath) {
        final Path entryName = Paths.get(relativePath);
        int index = 0;
        for (; index < entryName.getNameCount(); index++) {
            if (entryName.getName(index).toString().matches("jdk-?\\d.*")) {
                break;
            }
        }
        if (index + 1 >= entryName.getNameCount()) {
            // this happens on the top-level directories in the archive, which we are removing
            return null;
        }
        // finally remove the top-level directories from the output path
        return entryName.subpath(index + 1, entryName.getNameCount());
    }

    private static String configName(String... parts) {
        return String.join("_", parts);
    }

    private static String dependencyNotation(Jdk jdk) {
        String platformDep = jdk.platform().equals("darwin") || jdk.platform().equals("osx")
            ? (jdk.vendor().equals("adoptopenjdk") ? "mac" : "osx")
            : jdk.platform();
        String extension = jdk.platform().equals("windows") ? "zip" : "tar.gz";

        return jdk.vendor() + ":" + platformDep + ":" + jdk.baseVersion() + "@" + extension;
    }
}

