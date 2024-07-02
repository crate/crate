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

package org.elasticsearch.index.translog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class TranslogHeaderTests extends ESTestCase {

    @Test
    public void testCurrentHeaderVersion() throws Exception {
        final String translogUUID = UUIDs.randomBase64UUID();
        final TranslogHeader outHeader = new TranslogHeader(translogUUID, randomNonNegativeLong());
        final long generation = randomNonNegativeLong();
        final Path translogFile = createTempDir().resolve(Translog.getFilename(generation));
        try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            outHeader.write(channel);
            assertThat(outHeader.sizeInBytes()).isEqualTo((int)channel.position());
        }
        try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
            final TranslogHeader inHeader = TranslogHeader.read(translogUUID, translogFile, channel);
            assertThat(inHeader.getTranslogUUID()).isEqualTo(translogUUID);
            assertThat(inHeader.getPrimaryTerm()).isEqualTo(outHeader.getPrimaryTerm());
            assertThat(inHeader.sizeInBytes()).isEqualTo((int)channel.position());
        }
        assertThatThrownBy(() -> {
            try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
                TranslogHeader.read(randomValueOtherThan(translogUUID, UUIDs::randomBase64UUID), translogFile, channel);
            }
        }).isExactlyInstanceOf(TranslogCorruptedException.class)
            .hasMessageContaining("this translog file belongs to a different translog");
        TestTranslog.corruptFile(logger, random(), translogFile, false);
        assertThatThrownBy(() -> {
            try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
                TranslogHeader.read(randomBoolean() ? outHeader.getTranslogUUID() : UUIDs.randomBase64UUID(), translogFile, channel);
                final TranslogHeader translogHeader = TranslogHeader.read(outHeader.getTranslogUUID(), translogFile, channel);
                // succeeds if the corruption corrupted the version byte making this look like a v2 translog, because we don't check the
                // checksum on this version
                assertThat(translogHeader.getPrimaryTerm()).as("version " + TranslogHeader.VERSION_CHECKPOINTS + " translog").isEqualTo(SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
                throw new TranslogCorruptedException(translogFile.toString(), "adjusted translog version");
            } catch (IllegalStateException e) {
                // corruption corrupted the version byte making this look like a v2, v1 or v0 translog
                assertThat("version " + TranslogHeader.VERSION_CHECKPOINTS + "-or-earlier translog",
                    e.getMessage(), anyOf(containsString("pre-2.0 translog found"), containsString("pre-1.4 translog found"),
                        containsString("pre-6.3 translog found")));
                throw new TranslogCorruptedException(translogFile.toString(), "adjusted translog version", e);
            }
        }).isExactlyInstanceOf(TranslogCorruptedException.class)
            .hasMessageNotContaining("this translog file belongs to a different translog");
    }

    @Test
    public void testCorruptTranslogHeader() throws Exception {
        final String translogUUID = UUIDs.randomBase64UUID();
        final TranslogHeader outHeader = new TranslogHeader(translogUUID, randomNonNegativeLong());
        final long generation = randomNonNegativeLong();
        final Path translogLocation = createTempDir();
        final Path translogFile = translogLocation.resolve(Translog.getFilename(generation));
        try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            outHeader.write(channel);
            assertThat(outHeader.sizeInBytes()).isEqualTo((int) channel.position());
        }
        TestTranslog.corruptFile(logger, random(), translogFile, false);
        assertThatThrownBy(() -> {
            try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
                TranslogHeader.read(randomValueOtherThan(translogUUID, UUIDs::randomBase64UUID), translogFile, channel);
            }
        }).isInstanceOfAny(IllegalStateException.class, TranslogCorruptedException.class);
    }
}
