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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.List;

import org.apache.opendal.Entry;
import org.apache.opendal.Operator;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

public class AzureFileInputTest {

    @Test
    public void test_to_pre_glob_path() {
        var uris =
            List.of(
                "azblob://dir1/prefix*/*.json",
                "azblob://dir1/*/prefix2/prefix3/a.json",
                "azblob://dir1/prefix/p*x/*/*.json",
                "azblob://dir1.1/prefix/key*"
            );
        var preGlobURIs = uris.stream()
            .map(URI::create)
            .map(AzureFileInput::toPreGlobPath)
            .toList();
        assertThat(preGlobURIs).isEqualTo(List.of(
            "/dir1/",
            "/dir1/",
            "/dir1/prefix/",
            "/dir1.1/prefix/"
        ));
    }

    @Test
    public void test_expand_uri() throws Exception {
        // Dummy settings to pass validation, client response is mocked.
        Settings settings = Settings.builder()
            .put(AzureBlobStorageSettings.CONTAINER_SETTING.getKey(), "dummy")
            .put(AzureBlobStorageSettings.ACCOUNT_KEY_SETTING.getKey(), "dummy")
            .put(AzureBlobStorageSettings.ACCOUNT_NAME_SETTING.getKey(), "dummy")
            .put(AzureBlobStorageSettings.ENDPOINT_SETTING.getKey(), "dummy")
            .build();

        AzureFileInput azureFileInput = spy(
            new AzureFileInput(mock(SharedAsyncExecutor.class), URI.create("azblob://dir1/dir2/*"), settings)
        );
        Operator operator = mock(Operator.class);
        when(operator.list("/dir1/dir2/")).thenReturn(
            List.of(
                new Entry("dir1/dir2/match1.json", null),
                // Too many subdirectories, see https://cratedb.com/docs/crate/reference/en/latest/sql/statements/copy-from.html#uri-globbing
                new Entry("dir1/dir2/dir3/no_match.json", null),
                new Entry("dir2/dir1/no_match.json", null),
                new Entry("dir1/dir0/dir2/no_match.json", null)
            )
        );
        when(azureFileInput.operator()).thenReturn(operator);

        assertThat(azureFileInput.isGlobbed()).isTrue();
        assertThat(azureFileInput.expandUri())
            .containsExactly(URI.create("dir1/dir2/match1.json"));
    }
}
