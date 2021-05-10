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

import io.crate.gradle.plugins.jdk.transform.SymbolicLinkPreservingUntarTransform;
import io.crate.gradle.plugins.jdk.transform.UnzipTransform;
import org.gradle.api.GradleException;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.dsl.RepositoryHandler;
import org.gradle.api.artifacts.repositories.IvyArtifactRepository;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.internal.artifacts.ArtifactAttributes;

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
 *      vendor="adoptopenjdk"
 *      version="13.0.2+8"
 *      os="linux"
 *      arch="aarch64"
 *  }
 *  mac {
 *      vendor="adoptopenjdk"
 *      version="13.0.2+8"
 *      os="linux"
 *      arch="x64"
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
        project.getDependencies().getArtifactTypes().maybeCreate(ArtifactTypeDefinition.ZIP_TYPE);
        project.getDependencies().registerTransform(UnzipTransform.class, transformSpec -> {
            transformSpec.getFrom().attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.ZIP_TYPE);
            transformSpec.getTo().attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.DIRECTORY_TYPE);
        });

        ArtifactTypeDefinition tarArtifactTypeDefinition = project.getDependencies().getArtifactTypes().maybeCreate("tar.gz");
        project.getDependencies().registerTransform(SymbolicLinkPreservingUntarTransform.class, transformSpec -> {
            transformSpec.getFrom().attribute(ArtifactAttributes.ARTIFACT_FORMAT, tarArtifactTypeDefinition.getName());
            transformSpec.getTo().attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.DIRECTORY_TYPE);
        });

        NamedDomainObjectContainer<Jdk> jdksContainer = project.container(Jdk.class, name -> {
            Configuration configuration = project.getConfigurations().create("jdk_" + name);
            configuration.setCanBeConsumed(false);
            configuration.getAttributes().attribute(
                ArtifactAttributes.ARTIFACT_FORMAT,
                ArtifactTypeDefinition.DIRECTORY_TYPE
            );

            Jdk jdk = new Jdk(name, configuration, project.getObjects());
            configuration.defaultDependencies(dependencies -> {
                jdk.finalizeValues();
                setupRepository(project, jdk);
                dependencies.add(project.getDependencies().create(dependencyNotation(jdk)));
            });
            return jdk;
        });
        project.getExtensions().add(EXTENSION_NAME, jdksContainer);
    }

    private void setupRepository(Project project, Jdk jdk) {
        RepositoryHandler repositories = project.getRepositories();

        /*
         * Define the appropriate repository for the given JDK vendor and version
         *
         * For Oracle/OpenJDK/AdoptOpenJDK we define a repository per-version.
         */
        String repoName = REPO_NAME_PREFIX + jdk.vendor() + "_" + jdk.version();
        String repoUrl;
        String artifactPattern;

        if (jdk.vendor().equals("adoptopenjdk")) {
            repoUrl = "https://api.adoptopenjdk.net/v3/binary/version/";
            artifactPattern =
                "jdk-" +
                jdk.baseVersion() +
                "+" +
                jdk.build() +
                "/[module]/[classifier]/jdk/hotspot/normal/adoptopenjdk";
        } else {
            throw new GradleException("Unknown JDK vendor [" + jdk.vendor() + "]");
        }

        // Define the repository if we haven't already
        if (repositories.findByName(repoName) == null) {
            repositories.ivy(repo -> {
                repo.setName(repoName);
                repo.setUrl(repoUrl);
                repo.metadataSources(IvyArtifactRepository.MetadataSources::artifact);
                repo.patternLayout(layout -> layout.artifact(artifactPattern));
                repo.content(repositoryContentDescriptor -> repositoryContentDescriptor.includeGroup(groupName(jdk)));
            });
        }
    }

    @SuppressWarnings("unchecked")
    public static NamedDomainObjectContainer<Jdk> getContUnzipTransformainer(Project project) {
        return (NamedDomainObjectContainer<Jdk>) project.getExtensions().getByName(EXTENSION_NAME);
    }

    private static String dependencyNotation(Jdk jdk) {
        var extension = jdk.os().equals("windows") ? "zip" : "tar.gz";
        return groupName(jdk) +
               ":" + jdk.os() +
               ":" + jdk.baseVersion() +
               ":" + jdk.arch() +
               "@" + extension;
    }

    private static String groupName(Jdk jdk) {
        return jdk.vendor() + "_" + jdk.major();
    }
}
