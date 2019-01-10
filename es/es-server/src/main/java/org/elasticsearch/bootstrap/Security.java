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

package org.elasticsearch.bootstrap;

import org.elasticsearch.common.SuppressForbidden;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.AccessMode;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.security.Policy;
import java.security.URIParameter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Initializes SecurityManager with necessary permissions.
 * <br>
 * <h1>Initialization</h1>
 * The JVM is not initially started with security manager enabled,
 * instead we turn it on early in the startup process. This is a tradeoff
 * between security and ease of use:
 * <ul>
 *   <li>Assigns file permissions to user-configurable paths that can
 *       be specified from the command-line or {@code elasticsearch.yml}.</li>
 *   <li>Allows for some contained usage of native code that would not
 *       otherwise be permitted.</li>
 * </ul>
 * <br>
 * <h1>Permissions</h1>
 * Permissions use a policy file packaged as a resource, this file is
 * also used in tests. File permissions are generated dynamically and
 * combined with this policy file.
 * <p>
 * For each configured path, we ensure it exists and is accessible before
 * granting permissions, otherwise directory creation would require
 * permissions to parent directories.
 * <p>
 * In some exceptional cases, permissions are assigned to specific jars only,
 * when they are so dangerous that general code should not be granted the
 * permission, but there are extenuating circumstances.
 * <p>
 * Scripts (groovy) are assigned minimal permissions. This does not provide adequate
 * sandboxing, as these scripts still have access to ES classes, and could
 * modify members, etc that would cause bad things to happen later on their
 * behalf (no package protections are yet in place, this would need some
 * cleanups to the scripting apis). But still it can provide some defense for users
 * that enable dynamic scripting without being fully aware of the consequences.
 * <br>
 * <h1>Debugging Security</h1>
 * A good place to start when there is a problem is to turn on security debugging:
 * <pre>
 * ES_JAVA_OPTS="-Djava.security.debug=access,failure" bin/elasticsearch
 * </pre>
 * <p>
 * When running tests you have to pass it to the test runner like this:
 * <pre>
 * gradle test -Dtests.jvm.argline="-Djava.security.debug=access,failure" ...
 * </pre>
 * See <a href="https://docs.oracle.com/javase/7/docs/technotes/guides/security/troubleshooting-security.html">
 * Troubleshooting Security</a> for information.
 */
final class Security {
    /** no instantiation */
    private Security() {}

    /**
     * Reads and returns the specified {@code policyFile}.
     * <p>
     * Jar files listed in {@code codebases} location will be provided to the policy file via
     * a system property of the short name: e.g. <code>${codebase.joda-convert-1.2.jar}</code>
     * would map to full URL.
     */
    @SuppressForbidden(reason = "accesses fully qualified URLs to configure security")
    static Policy readPolicy(URL policyFile, Map<String, URL> codebases) {
        try {
            List<String> propertiesSet = new ArrayList<>();
            try {
                // set codebase properties
                for (Map.Entry<String,URL> codebase : codebases.entrySet()) {
                    String name = codebase.getKey();
                    URL url = codebase.getValue();

                    // We attempt to use a versionless identifier for each codebase. This assumes a specific version
                    // format in the jar filename. While we cannot ensure all jars in all plugins use this format, nonconformity
                    // only means policy grants would need to include the entire jar filename as they always have before.
                    String property = "codebase." + name;
                    String aliasProperty = "codebase." + name.replaceFirst("-\\d+\\.\\d+.*\\.jar", "");
                    if (aliasProperty.equals(property) == false) {
                        propertiesSet.add(aliasProperty);
                        String previous = System.setProperty(aliasProperty, url.toString());
                        if (previous != null) {
                            throw new IllegalStateException("codebase property already set: " + aliasProperty + " -> " + previous +
                                                            ", cannot set to " + url.toString());
                        }
                    }
                    propertiesSet.add(property);
                    String previous = System.setProperty(property, url.toString());
                    if (previous != null) {
                        throw new IllegalStateException("codebase property already set: " + property + " -> " + previous +
                                                        ", cannot set to " + url.toString());
                    }
                }
                return Policy.getInstance("JavaPolicy", new URIParameter(policyFile.toURI()));
            } finally {
                // clear codebase properties
                for (String property : propertiesSet) {
                    System.clearProperty(property);
                }
            }
        } catch (NoSuchAlgorithmException | URISyntaxException e) {
            throw new IllegalArgumentException("unable to parse policy file `" + policyFile + "`", e);
        }
    }

    /**
     * Ensures configured directory {@code path} exists.
     * @throws IOException if {@code path} exists, but is not a directory, not accessible, or broken symbolic link.
     */
    static void ensureDirectoryExists(Path path) throws IOException {
        // this isn't atomic, but neither is createDirectories.
        if (Files.isDirectory(path)) {
            // verify access, following links (throws exception if something is wrong)
            // we only check READ as a sanity test
            path.getFileSystem().provider().checkAccess(path.toRealPath(), AccessMode.READ);
        } else {
            // doesn't exist, or not a directory
            try {
                Files.createDirectories(path);
            } catch (FileAlreadyExistsException e) {
                // convert optional specific exception so the context is clear
                IOException e2 = new NotDirectoryException(path.toString());
                e2.addSuppressed(e);
                throw e2;
            }
        }
    }
}
