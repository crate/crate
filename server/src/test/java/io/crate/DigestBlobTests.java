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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.UUID;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.crate.blob.BlobContainer;
import io.crate.blob.DigestBlob;

public class DigestBlobTests extends ESTestCase {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Test
    public void testDigestBlobResumeHeadAndAddContent() throws IOException {

        String digest = "417de3231e23dcd6d224ff60918024bc6c59aa58";
        UUID transferId = UUID.randomUUID();
        int currentPos = 2;

        BlobContainer container = new BlobContainer(tmpFolder.newFolder().toPath());
        File filePath = new File(container.getTmpDirectory().toFile(), String.format(Locale.ENGLISH, "%s.%s", digest, transferId.toString()));
        if (filePath.exists()) {
            assertThat(filePath.delete()).isTrue();
        }

        DigestBlob digestBlob = DigestBlob.resumeTransfer(
            container, digest, transferId, currentPos);

        BytesArray contentHead = new BytesArray("A".getBytes(StandardCharsets.UTF_8));
        digestBlob.addToHead(contentHead);

        BytesArray contentTail = new BytesArray("CDEFGHIJKL".getBytes(StandardCharsets.UTF_8));
        digestBlob.addContent(contentTail, false);

        contentHead = new BytesArray("B".getBytes(StandardCharsets.UTF_8));
        digestBlob.addToHead(contentHead);

        contentTail = new BytesArray("MNO".getBytes(StandardCharsets.UTF_8));
        digestBlob.addContent(contentTail, true);

        // check if tmp file's content is correct
        byte[] buffer = new byte[15];
        try (FileInputStream stream = new FileInputStream(digestBlob.file())) {
            assertThat(stream.read(buffer, 0, 15)).isEqualTo(15);
            assertThat(new BytesArray(buffer).utf8ToString().trim()).isEqualTo("ABCDEFGHIJKLMNO");
        }

        File file = digestBlob.commit();
        // check if final file's content is correct
        buffer = new byte[15];

        try (FileInputStream stream = new FileInputStream(file)) {
            assertThat(stream.read(buffer, 0, 15)).isEqualTo(15);
            assertThat(new BytesArray(buffer).utf8ToString().trim()).isEqualTo("ABCDEFGHIJKLMNO");
        }

        // assert file created
        assertThat(file.exists()).isTrue();
        // just in case any references to file left
        assertThat(file.delete()).isTrue();
    }

    @Test
    public void testResumeDigestBlobAddHeadAfterContent() throws IOException {
        UUID transferId = UUID.randomUUID();
        BlobContainer container = new BlobContainer(tmpFolder.newFolder().toPath());
        DigestBlob digestBlob = DigestBlob.resumeTransfer(
            container, "417de3231e23dcd6d224ff60918024bc6c59aa58", transferId, 2);

        BytesArray contentTail = new BytesArray("CDEFGHIJKLMN".getBytes(StandardCharsets.UTF_8));
        digestBlob.addContent(contentTail, false);

        BytesArray contentHead = new BytesArray("AB".getBytes(StandardCharsets.UTF_8));
        digestBlob.addToHead(contentHead);

        contentTail = new BytesArray("O".getBytes(StandardCharsets.UTF_8));
        digestBlob.addContent(contentTail, true);

        // check if tmp file's content is correct
        byte[] buffer = new byte[15];
        try (FileInputStream stream = new FileInputStream(digestBlob.file())) {
            assertThat(stream.read(buffer, 0, 15)).isEqualTo(15);
            assertThat(new BytesArray(buffer).utf8ToString().trim()).isEqualTo("ABCDEFGHIJKLMNO");
        }

        File file = digestBlob.commit();

        // check if final file's content is correct
        buffer = new byte[15];
        try (FileInputStream stream = new FileInputStream(file)) {
            assertThat(stream.read(buffer, 0, 15)).isEqualTo(15);
            assertThat(new BytesArray(buffer).utf8ToString().trim()).isEqualTo("ABCDEFGHIJKLMNO");
        }

        // assert file created
        assertThat(file.exists()).isTrue();
        // just in case any references to file left
        assertThat(file.delete()).isTrue();
    }
}
