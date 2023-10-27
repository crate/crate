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

package io.crate;


import static io.crate.testing.Asserts.assertThat;

import java.util.List;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.protocols.http.HttpBlobHandler;

public class BlobHandlerTest extends ESTestCase {

    private static List<String> validUrls = List.of(
        "/_blobs/b/f3b4df9c032a14ad415455354798fa2fc3bf1df7",
        "/_blobs/bb/ab08643dd7fc5a3c0d8ebbd032a0b3605dd295a6",
        "/_blobs/looooooooooooooooong_blob_table_name/b2b72f87aa85ba8ca8a71c28c8d27f8a4541b55f"
    );
    private static List<String> invalidUrls = List.of(
        "_blobs/b/f3b4df9c032a14ad415455354798fa2fc3bf1df7",
        "/_blobs/b/f3b4df9c032a14ad415455354798fa2fc3bf1df7/",
        "/_blobs/b/f3b4df9c032a14ad415455354798fa2fc3bf1df",
        "/_blobs/f3b4df9c032a14ad415455354798fa2fc3bf1df7",
        "/_blobs//f3b4df9c032a14ad415455354798fa2fc3bf1df7",
        "/blobs/bb/ab08643dd7fc5a3c0d8ebbd032a0b3605dd295a6"
    );

    @Test
    public void testBlobHandlerRegex() throws Exception {
        for (String validUrl : validUrls) {
            assertThat(HttpBlobHandler.BLOBS_PATTERN.matcher(validUrl).matches()).isTrue();
        }
        for (String invalidUrl : invalidUrls) {
            assertThat(HttpBlobHandler.BLOBS_PATTERN.matcher(invalidUrl).matches()).isFalse();
        }
    }
}
