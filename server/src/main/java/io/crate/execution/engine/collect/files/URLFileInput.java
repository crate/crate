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

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

class URLFileInput implements FileInput {

    private final URI fileUri;

    public URLFileInput(URI fileUri) {
        // If the full fileUri contains a wildcard the fileUri passed as argument here is the fileUri up to the wildcard
        this.fileUri = fileUri;
    }

    @Override
    public boolean isGlobbed() {
        return false;
    }

    @Override
    public URI uri() {
        return fileUri;
    }

    @Override
    public List<URI> expandUri() throws IOException {
        // for URLs listing directory contents is not supported so always return the full fileUri for now
        return Collections.singletonList(this.fileUri);
    }

    @Override
    public InputStream getStream(URI uri) throws IOException {
        URL url = uri.toURL();
        Proxy proxy = getProxyForUrl(uri);
        URLConnection connection = url.openConnection(proxy);
        return connection.getInputStream();
    }

    @Override
    public boolean sharedStorageDefault() {
        return true;
    }

    /**
     * Creates a Proxy object based on JVM system properties for the given URI.
     * Respects http.proxyHost, http.proxyPort, https.proxyHost, https.proxyPort,
     * and http.nonProxyHosts system properties.
     *
     * @param uri The URI for which to determine proxy settings
     * @return A Proxy object configured based on system properties, or Proxy.NO_PROXY if no proxy is configured
     */
    private Proxy getProxyForUrl(URI uri) {
        String scheme = uri.getScheme();
        if (scheme == null || (!scheme.equalsIgnoreCase("http") && !scheme.equalsIgnoreCase("https"))) {
            return Proxy.NO_PROXY;
        }

        // Check if host should bypass proxy
        String host = uri.getHost();
        if (host != null && shouldBypassProxy(host)) {
            return Proxy.NO_PROXY;
        }

        // Get proxy settings based on protocol
        String proxyHost = System.getProperty(scheme.toLowerCase() + ".proxyHost");
        String proxyPortStr = System.getProperty(scheme.toLowerCase() + ".proxyPort");

        if (proxyHost == null || proxyHost.trim().isEmpty()) {
            return Proxy.NO_PROXY;
        }

        // Remove protocol prefix if present in proxyHost (e.g., "http://proxy.example.com" -> "proxy.example.com")
        if (proxyHost.startsWith("http://")) {
            proxyHost = proxyHost.substring(7);
        } else if (proxyHost.startsWith("https://")) {
            proxyHost = proxyHost.substring(8);
        }

        int proxyPort;
        try {
            proxyPort = proxyPortStr != null ? Integer.parseInt(proxyPortStr) : (scheme.equalsIgnoreCase("https") ? 443 : 80);
        } catch (NumberFormatException e) {
            // Default ports if parsing fails
            proxyPort = scheme.equalsIgnoreCase("https") ? 443 : 80;
        }

        return new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort));
    }

    /**
     * Checks if a host should bypass the proxy based on the http.nonProxyHosts system property.
     * The property uses pipe-separated patterns with * as wildcard.
     *
     * @param host The host to check
     * @return true if the host should bypass the proxy, false otherwise
     */
    private boolean shouldBypassProxy(String host) {
        String nonProxyHosts = System.getProperty("http.nonProxyHosts");
        if (nonProxyHosts == null || nonProxyHosts.trim().isEmpty()) {
            return false;
        }

        // Split by pipe and check each pattern
        String[] patterns = nonProxyHosts.split("\\|");
        for (String pattern : patterns) {
            pattern = pattern.trim();
            if (pattern.isEmpty()) {
                continue;
            }
            
            // Convert wildcard pattern to regex
            // Escape special regex characters except *
            String regexPattern = pattern
                .replace(".", "\\.")
                .replace("*", ".*");
            
            if (Pattern.matches(regexPattern, host)) {
                return true;
            }
        }
        return false;
    }
}