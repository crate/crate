/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.copy.azure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.util.List;

import org.junit.Test;

public class AzureURITest {

    private static final String COMMON_PREFIX = "az://myaccount.blob.core.windows.net/my-container";

    @Test
    public void test_port_is_added_when_defined() throws Exception {
        AzureURI azureURI = AzureURI.of(URI.create("az://myaccount.blob.core.windows.net:1234/container/file.json"));
        assertThat(azureURI.endpoint()).isEqualTo("myaccount.blob.core.windows.net:1234");

        // No port
        azureURI = AzureURI.of(URI.create("az://myaccount.blob.core.windows.net/container/file.json"));
        assertThat(azureURI.endpoint()).isEqualTo("myaccount.blob.core.windows.net");
    }

    @Test
    public void test_empty_path_after_container_throws_exception() throws Exception {
        URI uri = URI.create("az://myaccount.blob.core.windows.net/container/");
        assertThatThrownBy(() -> AzureURI.of(uri))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid URI. Path after container cannot be empty");
    }

    @Test
    public void test_host_contains_only_account_throws_exception() throws Exception {
        URI uri = URI.create("az://myaccount/my-container/dir");
        assertThatThrownBy(() -> AzureURI.of(uri))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid URI. URI must look like 'az://account.endpoint_suffix/container/path/to/file'");
    }

    @Test
    public void test_no_container_throws_exception() throws Exception {
        URI uri = URI.create("az://myaccount.blob.core.windows.net/dir");
        assertThatThrownBy(() -> AzureURI.of(uri))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid URI. URI must look like 'az://account.endpoint_suffix/container/path/to/file'");
    }

    @Test
    public void test_container_has_uppercase_letter_throws_exception() throws Exception {
        URI uri = URI.create("dummy://myaccount.blob.core.windows.net/Container/dir");
        assertThatThrownBy(() -> AzureURI.of(uri))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid URI. URI must look like 'az://account.endpoint_suffix/container/path/to/file'");
    }

    @Test
    public void test_container_has_invalid_length_throws_exception() throws Exception {
        URI uri = URI.create("dummy://myaccount.blob.core.windows.net/co/dir");
        assertThatThrownBy(() -> AzureURI.of(uri))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid URI. URI must look like 'az://account.endpoint_suffix/container/path/to/file'");
    }

    @Test
    public void test_invalid_scheme_throws_exception() throws Exception {
        URI uri = URI.create("dummy://myaccount.blob.core.windows.net/container/dir");
        assertThatThrownBy(() -> AzureURI.of(uri))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid URI. URI must look like 'az://account.endpoint_suffix/container/path/to/file'");
    }

    @Test
    public void test_match_glob_pattern() throws Exception {
        List<String> entries = List.of(
            "dir1/dir2/match1.json",
             // Too many subdirectories, see https://cratedb.com/docs/crate/reference/en/latest/sql/statements/copy-from.html#uri-globbing
            "dir1/dir2/dir3/no_match.json",
            "dir2/dir1/no_match.json",
            "dir1/dir0/dir2/no_match.json"
        );

        AzureURI azureURI = AzureURI.of(URI.create(COMMON_PREFIX + "/dir1/dir2/*"));

        assertThat(azureURI.preGlobPath()).isNotNull();

        List<String> matches = entries.stream().filter(azureURI::matchesGlob).toList();
        assertThat(matches).containsExactly("dir1/dir2/match1.json");
    }

    @Test
    public void test_to_pre_glob_path() {
        var uris =
            List.of(
                COMMON_PREFIX + "/dir1/prefix*/*.json",
                COMMON_PREFIX + "/dir1/*/prefix2/prefix3/a.json",
                COMMON_PREFIX + "/dir1/prefix/p*x/*/*.json",
                COMMON_PREFIX + "/dir1.1/prefix/key*",
                COMMON_PREFIX + "/*" // Glob matches the whole container
            );
        var preGlobURIs = uris.stream()
            .map(URI::create)
            .map(AzureURI::of)
            .map(AzureURI::preGlobPath)
            .toList();
        assertThat(preGlobURIs).isEqualTo(List.of(
            "/dir1/",
            "/dir1/",
            "/dir1/prefix/",
            "/dir1.1/prefix/",
            "/"
        ));

    }
}
