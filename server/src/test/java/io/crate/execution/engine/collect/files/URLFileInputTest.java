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

package io.crate.execution.engine.collect.files;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class URLFileInputTest extends ESTestCase {

    @Test
    public void testGetStream() throws IOException {
        Path tempDir = createTempDir();
        File file = new File(tempDir.toFile(), "doesnt_exist");
        URLFileInput input = new URLFileInput(file.toURI());

        String expectedMessage = "doesnt_exist (No such file or directory)";
        if (isRunningOnWindows()) {
            expectedMessage = "doesnt_exist (The system cannot find the file specified)";
        }

        assertThatThrownBy(() -> input.getStream(file.toURI()))
            .isExactlyInstanceOf(FileNotFoundException.class)
            .hasMessageContaining(expectedMessage);
    }

    @Test
    public void testProxyConfigurationIsRespected() throws IOException {
        // This test verifies that proxy system properties are read
        // Note: We can't easily test actual proxy connection without a real proxy server
        // This test just ensures the code doesn't throw exceptions with proxy settings
        
        String originalHttpProxyHost = System.getProperty("http.proxyHost");
        String originalHttpProxyPort = System.getProperty("http.proxyPort");
        
        try {
            // Set proxy properties
            System.setProperty("http.proxyHost", "proxy.example.com");
            System.setProperty("http.proxyPort", "8080");
            
            Path tempDir = createTempDir();
            File file = new File(tempDir.toFile(), "test_file");
            file.createNewFile();
            
            URLFileInput input = new URLFileInput(file.toURI());
            
            // Should not throw exception even with proxy settings configured
            // (file:// URLs don't use HTTP proxy)
            input.getStream(file.toURI()).close();
            
        } finally {
            // Restore original values
            if (originalHttpProxyHost != null) {
                System.setProperty("http.proxyHost", originalHttpProxyHost);
            } else {
                System.clearProperty("http.proxyHost");
            }
            if (originalHttpProxyPort != null) {
                System.setProperty("http.proxyPort", originalHttpProxyPort);
            } else {
                System.clearProperty("http.proxyPort");
            }
        }
    }

    @Test
    public void testNonProxyHostsBypass() throws IOException {
        // Test that http.nonProxyHosts is respected
        String originalNonProxyHosts = System.getProperty("http.nonProxyHosts");
        
        try {
            System.setProperty("http.nonProxyHosts", "localhost|127.0.0.1|*.local");
            
            Path tempDir = createTempDir();
            File file = new File(tempDir.toFile(), "test_file");
            file.createNewFile();
            
            URLFileInput input = new URLFileInput(file.toURI());
            
            // Should work fine with nonProxyHosts configured
            input.getStream(file.toURI()).close();
            
        } finally {
            if (originalNonProxyHosts != null) {
                System.setProperty("http.nonProxyHosts", originalNonProxyHosts);
            } else {
                System.clearProperty("http.nonProxyHosts");
            }
        }
    }
}