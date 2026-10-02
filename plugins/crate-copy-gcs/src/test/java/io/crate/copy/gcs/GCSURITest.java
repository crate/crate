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

package io.crate.copy.gcs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.util.List;

import org.junit.Test;

public class GCSURITest {

    private static final String COMMON_PREFIX = "gs://my-bucket";

    @Test
    public void test_valid_uri_parses_bucket_and_path() {
        GCSURI gcsURI = GCSURI.of(URI.create("gs://my-bucket/dir/file.json"));
        assertThat(gcsURI.bucket()).isEqualTo("my-bucket");
        assertThat(gcsURI.resourcePath()).isEqualTo("/dir/file.json");
    }

    @Test
    public void test_invalid_scheme_throws_exception() {
        URI uri = URI.create("dummy://bucket/path");
        assertThatThrownBy(() -> GCSURI.of(uri))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid URI. URI must look like 'gs://bucket/path/to/file'");
    }

    @Test
    public void test_missing_bucket_throws_exception() {
        URI uri = URI.create("gs:///path/to/file");
        assertThatThrownBy(() -> GCSURI.of(uri))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid URI. Bucket name is required: 'gs://bucket/path/to/file'");
    }

    @Test
    public void test_empty_path_throws_exception() {
        URI uri = URI.create("gs://bucket/");
        assertThatThrownBy(() -> GCSURI.of(uri))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid URI. Path after bucket cannot be empty");
    }

    @Test
    public void test_match_glob_pattern() {
        List<String> entries = List.of(
            "dir1/dir2/match1.json",
            "dir1/dir2/dir3/no_match.json",
            "dir2/dir1/no_match.json",
            "dir1/dir0/dir2/no_match.json"
        );

        GCSURI gcsURI = GCSURI.of(URI.create(COMMON_PREFIX + "/dir1/dir2/*"));

        assertThat(gcsURI.preGlobPath()).isNotNull();

        List<String> matches = entries.stream().filter(gcsURI::matchesGlob).toList();
        assertThat(matches).containsExactly("dir1/dir2/match1.json");
    }

    @Test
    public void test_pre_glob_path() {
        var uris =
            List.of(
                COMMON_PREFIX + "/dir1/prefix*/*.json",
                COMMON_PREFIX + "/dir1/*/prefix2/prefix3/a.json",
                COMMON_PREFIX + "/dir1/prefix/p*x/*/*.json",
                COMMON_PREFIX + "/dir1.1/prefix/key*",
                COMMON_PREFIX + "/*"
            );
        var preGlobURIs = uris.stream()
            .map(URI::create)
            .map(GCSURI::of)
            .map(GCSURI::preGlobPath)
            .toList();
        assertThat(preGlobURIs).isEqualTo(List.of(
            "/dir1/",
            "/dir1/",
            "/dir1/prefix/",
            "/dir1.1/prefix/",
            "/"
        ));
    }

    @Test
    public void test_no_glob_returns_null_pre_glob_path() {
        GCSURI gcsURI = GCSURI.of(URI.create("gs://bucket/dir/file.json"));
        assertThat(gcsURI.preGlobPath()).isNull();
    }
}
