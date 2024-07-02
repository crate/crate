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
package org.elasticsearch.index.store;

import static java.util.Collections.unmodifiableMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import io.crate.common.exceptions.Exceptions;
import io.crate.common.io.IOUtils;
import io.crate.common.unit.TimeValue;

public class StoreTests extends ESTestCase {

    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings("index",
        Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, org.elasticsearch.Version.CURRENT).build());
    private static final Version MIN_SUPPORTED_LUCENE_VERSION = org.elasticsearch.Version.CURRENT
        .minimumIndexCompatibilityVersion().luceneVersion;

    public void testRefCount() {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        IndexSettings indexSettings = INDEX_SETTINGS;
        Store store = new Store(shardId, indexSettings, StoreTests.newDirectory(random()), new DummyShardLock(shardId));
        int incs = randomIntBetween(1, 100);
        for (int i = 0; i < incs; i++) {
            if (randomBoolean()) {
                store.incRef();
            } else {
                assertThat(store.tryIncRef()).isTrue();
            }
            store.ensureOpen();
        }

        for (int i = 0; i < incs; i++) {
            store.decRef();
            store.ensureOpen();
        }

        store.incRef();
        store.close();
        for (int i = 0; i < incs; i++) {
            if (randomBoolean()) {
                store.incRef();
            } else {
                assertThat(store.tryIncRef()).isTrue();
            }
            store.ensureOpen();
        }

        for (int i = 0; i < incs; i++) {
            store.decRef();
            store.ensureOpen();
        }

        store.decRef();
        assertThat(store.refCount()).isEqualTo(0);
        assertThat(store.tryIncRef()).isFalse();
        assertThatThrownBy(store::incRef).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(store::ensureOpen).isInstanceOf(IllegalStateException.class);
    }

    public void testVerifyingIndexOutput() throws IOException {
        Directory dir = newDirectory();
        IndexOutput output = dir.createOutput("foo.bar", IOContext.DEFAULT);
        int iters = scaledRandomIntBetween(10, 100);
        for (int i = 0; i < iters; i++) {
            BytesRef bytesRef = new BytesRef(TestUtil.randomRealisticUnicodeString(random(), 10, 1024));
            output.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        }
        CodecUtil.writeFooter(output);
        output.close();
        IndexInput indexInput = dir.openInput("foo.bar", IOContext.DEFAULT);
        String checksum = Store.digestToString(CodecUtil.retrieveChecksum(indexInput));
        indexInput.seek(0);
        BytesRef ref = new BytesRef(scaledRandomIntBetween(1, 1024));
        long length = indexInput.length();
        IndexOutput verifyingOutput = new Store.LuceneVerifyingIndexOutput(new StoreFileMetadata("foo1.bar", length, checksum,
            MIN_SUPPORTED_LUCENE_VERSION), dir.createOutput("foo1.bar", IOContext.DEFAULT));
        while (length > 0) {
            if (random().nextInt(10) == 0) {
                verifyingOutput.writeByte(indexInput.readByte());
                length--;
            } else {
                int min = (int) Math.min(length, ref.bytes.length);
                indexInput.readBytes(ref.bytes, ref.offset, min);
                verifyingOutput.writeBytes(ref.bytes, ref.offset, min);
                length -= min;
            }
        }
        Store.verify(verifyingOutput);
        try {
            appendRandomData(verifyingOutput);
            fail("should be a corrupted index");
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            // ok
        }
        try {
            Store.verify(verifyingOutput);
            fail("should be a corrupted index");
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            // ok
        }

        IOUtils.close(indexInput, verifyingOutput, dir);
    }

    public void testVerifyingIndexOutputOnEmptyFile() throws IOException {
        Directory dir = newDirectory();
        IndexOutput verifyingOutput =
            new Store.LuceneVerifyingIndexOutput(new StoreFileMetadata("foo.bar", 0, Store.digestToString(0),
                MIN_SUPPORTED_LUCENE_VERSION),
                dir.createOutput("foo1.bar", IOContext.DEFAULT));
        try {
            Store.verify(verifyingOutput);
            fail("should be a corrupted index");
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            // ok
        }
        IOUtils.close(verifyingOutput, dir);
    }

    public void testChecksumCorrupted() throws IOException {
        Directory dir = newDirectory();
        IndexOutput output = dir.createOutput("foo.bar", IOContext.DEFAULT);
        int iters = scaledRandomIntBetween(10, 100);
        for (int i = 0; i < iters; i++) {
            BytesRef bytesRef = new BytesRef(TestUtil.randomRealisticUnicodeString(random(), 10, 1024));
            output.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        }
        output.writeInt(CodecUtil.FOOTER_MAGIC);
        output.writeInt(0);
        String checksum = Store.digestToString(output.getChecksum());
        output.writeLong(output.getChecksum() + 1); // write a wrong checksum to the file
        output.close();

        IndexInput indexInput = dir.openInput("foo.bar", IOContext.DEFAULT);
        indexInput.seek(0);
        BytesRef ref = new BytesRef(scaledRandomIntBetween(1, 1024));
        long length = indexInput.length();
        IndexOutput verifyingOutput = new Store.LuceneVerifyingIndexOutput(new StoreFileMetadata("foo1.bar", length, checksum,
            MIN_SUPPORTED_LUCENE_VERSION), dir.createOutput("foo1.bar", IOContext.DEFAULT));
        length -= 8; // we write the checksum in the try / catch block below
        while (length > 0) {
            if (random().nextInt(10) == 0) {
                verifyingOutput.writeByte(indexInput.readByte());
                length--;
            } else {
                int min = (int) Math.min(length, ref.bytes.length);
                indexInput.readBytes(ref.bytes, ref.offset, min);
                verifyingOutput.writeBytes(ref.bytes, ref.offset, min);
                length -= min;
            }
        }

        try {
            BytesRef checksumBytes = new BytesRef(8);
            checksumBytes.length = 8;
            indexInput.readBytes(checksumBytes.bytes, checksumBytes.offset, checksumBytes.length);
            if (randomBoolean()) {
                verifyingOutput.writeBytes(checksumBytes.bytes, checksumBytes.offset, checksumBytes.length);
            } else {
                for (int i = 0; i < checksumBytes.length; i++) {
                    verifyingOutput.writeByte(checksumBytes.bytes[i]);
                }
            }
            fail("should be a corrupted index");
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            // ok
        }
        IOUtils.close(indexInput, verifyingOutput, dir);
    }

    private void appendRandomData(IndexOutput output) throws IOException {
        int numBytes = randomIntBetween(1, 1024);
        final BytesRef ref = new BytesRef(scaledRandomIntBetween(1, numBytes));
        ref.length = ref.bytes.length;
        while (numBytes > 0) {
            if (random().nextInt(10) == 0) {
                output.writeByte(randomByte());
                numBytes--;
            } else {
                for (int i = 0; i<ref.length; i++) {
                    ref.bytes[i] = randomByte();
                }
                final int min = Math.min(numBytes, ref.bytes.length);
                output.writeBytes(ref.bytes, ref.offset, min);
                numBytes -= min;
            }
        }
    }

    public void testVerifyingIndexOutputWithBogusInput() throws IOException {
        Directory dir = newDirectory();
        int length = scaledRandomIntBetween(10, 1024);
        IndexOutput verifyingOutput = new Store.LuceneVerifyingIndexOutput(new StoreFileMetadata("foo1.bar", length, "",
            MIN_SUPPORTED_LUCENE_VERSION), dir.createOutput("foo1.bar", IOContext.DEFAULT));
        try {
            while (length > 0) {
                verifyingOutput.writeByte((byte) random().nextInt());
                length--;
            }
            fail("should be a corrupted index");
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            // ok
        }
        IOUtils.close(verifyingOutput, dir);
    }

    public void testNewChecksums() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        Store store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), new DummyShardLock(shardId));
        // set default codec - all segments need checksums
        IndexWriter writer = new IndexWriter(store.directory(), newIndexWriterConfig(random(),
            new MockAnalyzer(random())).setCodec(TestUtil.getDefaultCodec()));
        int docs = 1 + random().nextInt(100);

        for (int i = 0; i < docs; i++) {
            Document doc = new Document();
            doc.add(new TextField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(new TextField("body",
                TestUtil.randomRealisticUnicodeString(random()), random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(new SortedDocValuesField("dv", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));
            writer.addDocument(doc);
        }
        if (random().nextBoolean()) {
            for (int i = 0; i < docs; i++) {
                if (random().nextBoolean()) {
                    Document doc = new Document();
                    doc.add(new TextField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
                    doc.add(new TextField("body",
                        TestUtil.randomRealisticUnicodeString(random()), random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
                    writer.updateDocument(new Term("id", "" + i), doc);
                }
            }
        }
        if (random().nextBoolean()) {
            DirectoryReader.open(writer).close(); // flush
        }
        Store.MetadataSnapshot metadata;
        // check before we committed
        try {
            store.getMetadata(null);
            fail("no index present - expected exception");
        } catch (IndexNotFoundException ex) {
            // expected
        }
        writer.commit();
        writer.close();
        metadata = store.getMetadata(null);
        assertThat(metadata.asMap().isEmpty()).isFalse();
        for (StoreFileMetadata meta : metadata) {
            try (IndexInput input = store.directory().openInput(meta.name(), IOContext.DEFAULT)) {
                String checksum = Store.digestToString(CodecUtil.retrieveChecksum(input));
                assertThat(meta.checksum()).as("File: " + meta.name() + " has a different checksum").isEqualTo(checksum);
                assertThat(meta.writtenBy()).isEqualTo(Version.LATEST);
                if (meta.name().endsWith(".si") || meta.name().startsWith("segments_")) {
                    assertThat(meta.hash().length, greaterThan(0));
                }
            }
        }
        assertConsistent(store, metadata);

        TestUtil.checkIndex(store.directory());
        assertDeleteContent(store, store.directory());
        IOUtils.close(store);
    }

    public void testCheckIntegrity() throws IOException {
        Directory dir = newDirectory();
        long luceneFileLength = 0;

        try (IndexOutput output = dir.createOutput("lucene_checksum.bin", IOContext.DEFAULT)) {
            int iters = scaledRandomIntBetween(10, 100);
            for (int i = 0; i < iters; i++) {
                BytesRef bytesRef = new BytesRef(TestUtil.randomRealisticUnicodeString(random(), 10, 1024));
                output.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                luceneFileLength += bytesRef.length;
            }
            CodecUtil.writeFooter(output);
            luceneFileLength += CodecUtil.footerLength();

        }

        try (IndexInput indexInput = dir.openInput("lucene_checksum.bin", IOContext.DEFAULT)) {
            assertThat(indexInput.length()).isEqualTo(luceneFileLength);
        }

        dir.close();

    }

    public void testVerifyingIndexInput() throws IOException {
        Directory dir = newDirectory();
        IndexOutput output = dir.createOutput("foo.bar", IOContext.DEFAULT);
        int iters = scaledRandomIntBetween(10, 100);
        for (int i = 0; i < iters; i++) {
            BytesRef bytesRef = new BytesRef(TestUtil.randomRealisticUnicodeString(random(), 10, 1024));
            output.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        }
        CodecUtil.writeFooter(output);
        output.close();

        // Check file
        IndexInput indexInput = dir.openInput("foo.bar", IOContext.DEFAULT);
        long checksum = CodecUtil.retrieveChecksum(indexInput);
        indexInput.seek(0);
        IndexInput verifyingIndexInput = new Store.VerifyingIndexInput(dir.openInput("foo.bar", IOContext.DEFAULT));
        readIndexInputFullyWithRandomSeeks(verifyingIndexInput);
        Store.verify(verifyingIndexInput);
        assertThat(checksum).isEqualTo(((ChecksumIndexInput) verifyingIndexInput).getChecksum());
        IOUtils.close(indexInput, verifyingIndexInput);

        // Corrupt file and check again
        corruptFile(dir, "foo.bar", "foo1.bar");
        verifyingIndexInput = new Store.VerifyingIndexInput(dir.openInput("foo1.bar", IOContext.DEFAULT));
        readIndexInputFullyWithRandomSeeks(verifyingIndexInput);
        try {
            Store.verify(verifyingIndexInput);
            fail("should be a corrupted index");
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            // ok
        }
        IOUtils.close(verifyingIndexInput);
        IOUtils.close(dir);
    }

    private void readIndexInputFullyWithRandomSeeks(IndexInput indexInput) throws IOException {
        BytesRef ref = new BytesRef(scaledRandomIntBetween(1, 1024));
        long pos = 0;
        while (pos < indexInput.length()) {
            assertThat(indexInput.getFilePointer()).isEqualTo(pos);
            int op = random().nextInt(5);
            if (op == 0) {
                int shift = 100 - randomIntBetween(0, 200);
                pos = Math.min(indexInput.length() - 1, Math.max(0, pos + shift));
                indexInput.seek(pos);
            } else if (op == 1) {
                indexInput.readByte();
                pos++;
            } else {
                int min = (int) Math.min(indexInput.length() - pos, ref.bytes.length);
                indexInput.readBytes(ref.bytes, ref.offset, min);
                pos += min;
            }
        }
    }

    private void corruptFile(Directory dir, String fileIn, String fileOut) throws IOException {
        IndexInput input = dir.openInput(fileIn, IOContext.READONCE);
        IndexOutput output = dir.createOutput(fileOut, IOContext.DEFAULT);
        long len = input.length();
        byte[] b = new byte[1024];
        long broken = randomInt((int) len-1);
        long pos = 0;
        while (pos < len) {
            int min = (int) Math.min(input.length() - pos, b.length);
            input.readBytes(b, 0, min);
            if (broken >= pos && broken < pos + min) {
                // Flip one byte
                int flipPos = (int) (broken - pos);
                b[flipPos] = (byte) (b[flipPos] ^ 42);
            }
            output.writeBytes(b, min);
            pos += min;
        }
        IOUtils.close(input, output);

    }

    public void assertDeleteContent(Store store, Directory dir) throws IOException {
        deleteContent(store.directory());
        assertThat(store.directory().listAll().length).as(Arrays.toString(store.directory().listAll())).isEqualTo(0);
        assertThat(store.stats(0L).sizeInBytes()).isEqualTo(0L);
        assertThat(dir.listAll().length).isEqualTo(0);
    }

    public static void assertConsistent(Store store, Store.MetadataSnapshot metadata) throws IOException {
        for (String file : store.directory().listAll()) {
            if (!IndexWriter.WRITE_LOCK_NAME.equals(file) && file.startsWith("extra") == false) {
                assertThat(metadata.asMap().containsKey(file)).as(file + " is not in the map: " + metadata.asMap().size() + " vs. " +
                    store.directory().listAll().length).isTrue();
            } else {
                assertThat(metadata.asMap().containsKey(file)).as(file + " is not in the map: " + metadata.asMap().size() + " vs. " +
                    store.directory().listAll().length).isFalse();
            }
        }
    }

    public void testRecoveryDiff() throws IOException, InterruptedException {
        int numDocs = 2 + random().nextInt(100);
        List<Document> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(new TextField("body",
                TestUtil.randomRealisticUnicodeString(random()), random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(new SortedDocValuesField("dv", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));
            docs.add(doc);
        }
        long seed = random().nextLong();
        Store.MetadataSnapshot first;
        {
            Random random = new Random(seed);
            IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random)).setCodec(TestUtil.getDefaultCodec());
            iwc.setMergePolicy(NoMergePolicy.INSTANCE);
            iwc.setUseCompoundFile(random.nextBoolean());
            final ShardId shardId = new ShardId("index", "_na_", 1);
            Store store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), new DummyShardLock(shardId));
            IndexWriter writer = new IndexWriter(store.directory(), iwc);
            final boolean lotsOfSegments = rarely(random);
            for (Document d : docs) {
                writer.addDocument(d);
                if (lotsOfSegments && random.nextBoolean()) {
                    writer.commit();
                } else if (rarely(random)) {
                    writer.commit();
                }
            }
            writer.commit();
            writer.close();
            first = store.getMetadata(null);
            assertDeleteContent(store, store.directory());
            store.close();
        }
        long time = new Date().getTime();
        while (time == new Date().getTime()) {
            Thread.sleep(10); // bump the time
        }
        Store.MetadataSnapshot second;
        Store store;
        {
            Random random = new Random(seed);
            IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random)).setCodec(TestUtil.getDefaultCodec());
            iwc.setMergePolicy(NoMergePolicy.INSTANCE);
            iwc.setUseCompoundFile(random.nextBoolean());
            final ShardId shardId = new ShardId("index", "_na_", 1);
            store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), new DummyShardLock(shardId));
            IndexWriter writer = new IndexWriter(store.directory(), iwc);
            final boolean lotsOfSegments = rarely(random);
            for (Document d : docs) {
                writer.addDocument(d);
                if (lotsOfSegments && random.nextBoolean()) {
                    writer.commit();
                } else if (rarely(random)) {
                    writer.commit();
                }
            }
            writer.commit();
            writer.close();
            second = store.getMetadata(null);
        }
        Store.RecoveryDiff diff = first.recoveryDiff(second);
        assertThat(first).hasSize(second.size());
        for (StoreFileMetadata md : first) {
            assertThat(second.get(md.name())).isNotNull();
            // si files are different - containing timestamps etc
            assertThat(second.get(md.name()).isSame(md)).isFalse();
        }
        assertThat(diff.different).hasSize(first.size());
        assertThat(diff.identical).hasSize(0); // in lucene 5 nothing is identical - we use random ids in file headers
        assertThat(diff.missing, empty());

        // check the self diff
        Store.RecoveryDiff selfDiff = first.recoveryDiff(first);
        assertThat(selfDiff.identical).hasSize(first.size());
        assertThat(selfDiff.different, empty());
        assertThat(selfDiff.missing, empty());


        // lets add some deletes
        Random random = new Random(seed);
        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random)).setCodec(TestUtil.getDefaultCodec());
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setUseCompoundFile(random.nextBoolean());
        iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
        IndexWriter writer = new IndexWriter(store.directory(), iwc);
        writer.deleteDocuments(new Term("id", Integer.toString(random().nextInt(numDocs))));
        writer.commit();
        writer.close();
        Store.MetadataSnapshot metadata = store.getMetadata(null);
        StoreFileMetadata delFile = null;
        for (StoreFileMetadata md : metadata) {
            if (md.name().endsWith(".liv")) {
                delFile = md;
                break;
            }
        }
        Store.RecoveryDiff afterDeleteDiff = metadata.recoveryDiff(second);
        if (delFile != null) {
            assertThat(afterDeleteDiff.identical).hasSize(metadata.size() - 2); // segments_N + del file
            assertThat(afterDeleteDiff.different).hasSize(0);
            assertThat(afterDeleteDiff.missing).hasSize(2);
        } else {
            // an entire segment must be missing (single doc segment got dropped)
            assertThat(afterDeleteDiff.identical.size(), greaterThan(0));
            assertThat(afterDeleteDiff.different).hasSize(0);
            assertThat(afterDeleteDiff.missing).hasSize(1); // the commit file is different
        }

        // check the self diff
        selfDiff = metadata.recoveryDiff(metadata);
        assertThat(selfDiff.identical).hasSize(metadata.size());
        assertThat(selfDiff.different, empty());
        assertThat(selfDiff.missing, empty());

        // add a new commit
        iwc = new IndexWriterConfig(new MockAnalyzer(random)).setCodec(TestUtil.getDefaultCodec());
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setUseCompoundFile(true); // force CFS - easier to test here since we know it will add 3 files
        iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
        writer = new IndexWriter(store.directory(), iwc);
        writer.addDocument(docs.get(0));
        writer.close();

        Store.MetadataSnapshot newCommitMetadata = store.getMetadata(null);
        Store.RecoveryDiff newCommitDiff = newCommitMetadata.recoveryDiff(metadata);
        if (delFile != null) {
            assertThat(newCommitDiff.identical).hasSize(newCommitMetadata.size() - 5); // segments_N, del file, cfs, cfe, si for the new segment
            assertThat(newCommitDiff.different).hasSize(1); // the del file must be different
            assertThat(newCommitDiff.different.get(0).name(), endsWith(".liv"));
            assertThat(newCommitDiff.missing).hasSize(4); // segments_N,cfs, cfe, si for the new segment
        } else {
            assertThat(newCommitDiff.identical).hasSize(newCommitMetadata.size() - 4); // segments_N, cfs, cfe, si for the new segment
            assertThat(newCommitDiff.different).hasSize(0);
            assertThat(newCommitDiff.missing).hasSize(4); // an entire segment must be missing (single doc segment got dropped)  plus the commit is different
        }

        deleteContent(store.directory());
        IOUtils.close(store);
    }

    public void testCleanupFromSnapshot() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        Store store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), new DummyShardLock(shardId));
        // this time random codec....
        IndexWriterConfig indexWriterConfig =
            newIndexWriterConfig(random(), new MockAnalyzer(random())).setCodec(TestUtil.getDefaultCodec());
        // we keep all commits and that allows us clean based on multiple snapshots
        indexWriterConfig.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
        IndexWriter writer = new IndexWriter(store.directory(), indexWriterConfig);
        int docs = 1 + random().nextInt(100);
        int numCommits = 0;
        for (int i = 0; i < docs; i++) {
            if (i > 0 && randomIntBetween(0, 10) == 0) {
                writer.commit();
                numCommits++;
            }
            Document doc = new Document();
            doc.add(new TextField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(new TextField("body",
                TestUtil.randomRealisticUnicodeString(random()), random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(new SortedDocValuesField("dv", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));
            writer.addDocument(doc);

        }
        if (numCommits < 1) {
            writer.commit();
            Document doc = new Document();
            doc.add(new TextField("id", "" + docs++, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(new TextField("body",
                TestUtil.randomRealisticUnicodeString(random()), random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(new SortedDocValuesField("dv", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));
            writer.addDocument(doc);
        }

        Store.MetadataSnapshot firstMeta = store.getMetadata(null);

        if (random().nextBoolean()) {
            for (int i = 0; i < docs; i++) {
                if (random().nextBoolean()) {
                    Document doc = new Document();
                    doc.add(new TextField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
                    doc.add(new TextField("body",
                        TestUtil.randomRealisticUnicodeString(random()), random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
                    writer.updateDocument(new Term("id", "" + i), doc);
                }
            }
        }
        writer.commit();
        writer.close();

        Store.MetadataSnapshot secondMeta = store.getMetadata(null);


        if (randomBoolean()) {
            store.cleanupAndVerify("test", firstMeta);
            String[] strings = store.directory().listAll();
            int numNotFound = 0;
            for (String file : strings) {
                if (file.startsWith("extra")) {
                    continue;
                }
                assertThat(firstMeta.contains(file) || file.equals("write.lock")).isTrue();
                if (secondMeta.contains(file) == false) {
                    numNotFound++;
                }

            }
            assertThat(numNotFound > 0).as("at least one file must not be in here since we have two commits?").isTrue();
        } else {
            store.cleanupAndVerify("test", secondMeta);
            String[] strings = store.directory().listAll();
            int numNotFound = 0;
            for (String file : strings) {
                if (file.startsWith("extra")) {
                    continue;
                }
                assertThat(secondMeta.contains(file) || file.equals("write.lock")).as(file).isTrue();
                if (firstMeta.contains(file) == false) {
                    numNotFound++;
                }

            }
            assertThat(numNotFound > 0).as("at least one file must not be in here since we have two commits?").isTrue();
        }

        deleteContent(store.directory());
        IOUtils.close(store);
    }

    public void testOnCloseCallback() throws IOException {
        final ShardId shardId =
            new ShardId(new Index(randomRealisticUnicodeOfCodepointLengthBetween(1, 10), "_na_"), randomIntBetween(0, 100));
        final AtomicInteger count = new AtomicInteger(0);
        final ShardLock lock = new DummyShardLock(shardId);

        Store store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), lock, theLock -> {
            assertThat(theLock.getShardId()).isEqualTo(shardId);
            assertThat(theLock).isEqualTo(lock);
            count.incrementAndGet();
        });
        assertThat(0).isEqualTo(count.get());

        final int iters = randomIntBetween(1, 10);
        for (int i = 0; i < iters; i++) {
            store.close();
        }

        assertThat(1).isEqualTo(count.get());
    }

    public void testStoreStats() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        Settings settings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, org.elasticsearch.Version.CURRENT)
                .put(Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMinutes(0)).build();
        Store store = new Store(shardId, IndexSettingsModule.newIndexSettings("index", settings), StoreTests.newDirectory(random()),
            new DummyShardLock(shardId));
        long initialStoreSize = 0;
        for (String extraFiles : store.directory().listAll()) {
            assertThat(extraFiles.startsWith("extra")).as("expected extraFS file but got: " + extraFiles).isTrue();
            initialStoreSize += store.directory().fileLength(extraFiles);
        }
        final long reservedBytes =  randomBoolean() ? StoreStats.UNKNOWN_RESERVED_BYTES :randomLongBetween(0L, Integer.MAX_VALUE);
        StoreStats stats = store.stats(reservedBytes);
        assertThat(stats.getSize().getBytes()).isEqualTo(initialStoreSize);
        assertThat(stats.getReservedSize().getBytes()).isEqualTo(reservedBytes);

        stats.add(null);
        assertThat(stats.getSize().getBytes()).isEqualTo(initialStoreSize);
        assertThat(stats.getReservedSize().getBytes()).isEqualTo(reservedBytes);

        final long otherStatsBytes = randomLongBetween(0L, Integer.MAX_VALUE);
        final long otherStatsReservedBytes = randomBoolean() ? StoreStats.UNKNOWN_RESERVED_BYTES :randomLongBetween(0L, Integer.MAX_VALUE);
        stats.add(new StoreStats(otherStatsBytes, otherStatsReservedBytes));
        assertThat(stats.getSize().getBytes()).isEqualTo(initialStoreSize + otherStatsBytes);
        assertThat(stats.getReservedSize().getBytes()).isEqualTo(Math.max(reservedBytes, 0L) + Math.max(otherStatsReservedBytes, 0L));

        Directory dir = store.directory();
        final long length;
        try (IndexOutput output = dir.createOutput("foo.bar", IOContext.DEFAULT)) {
            int iters = scaledRandomIntBetween(10, 100);
            for (int i = 0; i < iters; i++) {
                BytesRef bytesRef = new BytesRef(TestUtil.randomRealisticUnicodeString(random(), 10, 1024));
                output.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
            }
            length = output.getFilePointer();
        }

        assertThat(numNonExtraFiles(store) > 0).isTrue();
        stats = store.stats(0L);
        assertThat(length + initialStoreSize).isEqualTo(stats.getSizeInBytes());

        deleteContent(store.directory());
        IOUtils.close(store);
    }


    public static void deleteContent(Directory directory) throws IOException {
        final String[] files = directory.listAll();
        final List<IOException> exceptions = new ArrayList<>();
        for (String file : files) {
            try {
                directory.deleteFile(file);
            } catch (NoSuchFileException | FileNotFoundException e) {
                // ignore
            } catch (IOException e) {
                exceptions.add(e);
            }
        }
        Exceptions.rethrowAndSuppress(exceptions);
    }

    public int numNonExtraFiles(Store store) throws IOException {
        int numNonExtra = 0;
        for (String file : store.directory().listAll()) {
            if (file.startsWith("extra") == false) {
                numNonExtra++;
            }
        }
        return numNonExtra;
    }

    public void testMetadataSnapshotStreaming() throws Exception {
        Store.MetadataSnapshot outMetadataSnapshot = createMetadataSnapshot();
        org.elasticsearch.Version targetNodeVersion = randomVersion(random());

        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        out.setVersion(targetNodeVersion);
        outMetadataSnapshot.writeTo(out);

        ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput in = new InputStreamStreamInput(inBuffer);
        in.setVersion(targetNodeVersion);
        Store.MetadataSnapshot inMetadataSnapshot = new Store.MetadataSnapshot(in);
        Map<String, StoreFileMetadata> origEntries = new HashMap<>();
        origEntries.putAll(outMetadataSnapshot.asMap());
        for (Map.Entry<String, StoreFileMetadata> entry : inMetadataSnapshot.asMap().entrySet()) {
            assertThat(entry.getValue().name()).isEqualTo(origEntries.remove(entry.getKey()).name());
        }
        assertThat(origEntries).hasSize(0);
        assertThat(inMetadataSnapshot.getCommitUserData()).isEqualTo(outMetadataSnapshot.getCommitUserData());
    }

    protected Store.MetadataSnapshot createMetadataSnapshot() {
        StoreFileMetadata storeFileMetadata1 =
            new StoreFileMetadata("segments", 1, "666", MIN_SUPPORTED_LUCENE_VERSION);
        StoreFileMetadata storeFileMetadata2 =
            new StoreFileMetadata("no_segments", 1, "666", MIN_SUPPORTED_LUCENE_VERSION);
        Map<String, StoreFileMetadata> storeFileMetadataMap = new HashMap<>();
        storeFileMetadataMap.put(storeFileMetadata1.name(), storeFileMetadata1);
        storeFileMetadataMap.put(storeFileMetadata2.name(), storeFileMetadata2);
        return new Store.MetadataSnapshot(unmodifiableMap(storeFileMetadataMap), Map.of("userdata_1", "test", "userdata_2", "test"), 0);
    }

    public void testUserDataRead() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        Store store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), new DummyShardLock(shardId));
        IndexWriterConfig config = newIndexWriterConfig(random(), new MockAnalyzer(random())).setCodec(TestUtil.getDefaultCodec());
        SnapshotDeletionPolicy deletionPolicy = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
        config.setIndexDeletionPolicy(deletionPolicy);
        IndexWriter writer = new IndexWriter(store.directory(), config);
        Document doc = new Document();
        doc.add(new TextField("id", "1", Field.Store.NO));
        writer.addDocument(doc);
        Map<String, String> commitData = new HashMap<>(2);
        String syncId = "a sync id";
        commitData.put(Engine.SYNC_COMMIT_ID, syncId);
        writer.setLiveCommitData(commitData.entrySet());
        writer.commit();
        writer.close();
        Store.MetadataSnapshot metadata;
        metadata = store.getMetadata(randomBoolean() ? null : deletionPolicy.snapshot());
        assertThat(metadata.asMap().isEmpty()).isFalse();
        // do not check for correct files, we have enough tests for that above
        assertThat(metadata.getCommitUserData().get(Engine.SYNC_COMMIT_ID)).isEqualTo(syncId);
        TestUtil.checkIndex(store.directory());
        assertDeleteContent(store, store.directory());
        IOUtils.close(store);
    }

    public void testStreamStoreFilesMetadata() throws Exception {
        Store.MetadataSnapshot metadataSnapshot = createMetadataSnapshot();
        int numOfLeases = randomIntBetween(0, 10);
        List<RetentionLease> peerRecoveryRetentionLeases = new ArrayList<>();
        for (int i = 0; i < numOfLeases; i++) {
            peerRecoveryRetentionLeases.add(new RetentionLease(ReplicationTracker.getPeerRecoveryRetentionLeaseId(UUIDs.randomBase64UUID()),
                randomNonNegativeLong(), randomNonNegativeLong(), ReplicationTracker.PEER_RECOVERY_RETENTION_LEASE_SOURCE));
        }
        TransportNodesListShardStoreMetadata.StoreFilesMetadata outStoreFileMetadata =
            new TransportNodesListShardStoreMetadata.StoreFilesMetadata(new ShardId("test", "_na_", 0),
                metadataSnapshot, peerRecoveryRetentionLeases);
        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        org.elasticsearch.Version targetNodeVersion = randomVersion(random());
        out.setVersion(targetNodeVersion);
        outStoreFileMetadata.writeTo(out);
        ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput in = new InputStreamStreamInput(inBuffer);
        in.setVersion(targetNodeVersion);
        TransportNodesListShardStoreMetadata.StoreFilesMetadata inStoreFileMetadata =
            new TransportNodesListShardStoreMetadata.StoreFilesMetadata(in);
        Iterator<StoreFileMetadata> outFiles = outStoreFileMetadata.iterator();
        for (StoreFileMetadata inFile : inStoreFileMetadata) {
            assertThat(inFile.name()).isEqualTo(outFiles.next().name());
        }
        assertThat(outStoreFileMetadata.syncId()).isEqualTo(inStoreFileMetadata.syncId());
        assertThat(outStoreFileMetadata.peerRecoveryRetentionLeases()).isEqualTo(peerRecoveryRetentionLeases);
    }

    public void testMarkCorruptedOnTruncatedSegmentsFile() throws IOException {
        IndexWriterConfig iwc = newIndexWriterConfig();
        final ShardId shardId = new ShardId("index", "_na_", 1);
        Store store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), new DummyShardLock(shardId));
        IndexWriter writer = new IndexWriter(store.directory(), iwc);

        int numDocs = 1 + random().nextInt(10);
        List<Document> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(new TextField("body",
                TestUtil.randomRealisticUnicodeString(random()), random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(new SortedDocValuesField("dv", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));
            docs.add(doc);
        }
        for (Document d : docs) {
            writer.addDocument(d);
        }
        writer.commit();
        writer.close();
        SegmentInfos segmentCommitInfos = store.readLastCommittedSegmentsInfo();
        store.directory().deleteFile(segmentCommitInfos.getSegmentsFileName());
        try (IndexOutput out = store.directory().createOutput(segmentCommitInfos.getSegmentsFileName(), IOContext.DEFAULT)) {
            // empty file
        }

        try {
            if (randomBoolean()) {
                store.getMetadata(null);
            } else {
                store.readLastCommittedSegmentsInfo();
            }
            fail("corrupted segments_N file");
        } catch (CorruptIndexException ex) {
            // expected
        }
        assertThat(store.isMarkedCorrupted()).isTrue();
        // we have to remove the index since it's corrupted and might fail the MocKDirWrapper checkindex call
        Lucene.cleanLuceneIndex(store.directory());
        store.close();
    }

    public void testCanOpenIndex() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        IndexWriterConfig iwc = newIndexWriterConfig();
        Path tempDir = createTempDir();
        final BaseDirectoryWrapper dir = newFSDirectory(tempDir);
        assertThat(StoreUtils.canOpenIndex(logger, tempDir, shardId, (id, l, d) -> new DummyShardLock(id))).isFalse();
        IndexWriter writer = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new StringField("id", "1", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();
        writer.close();
        assertThat(StoreUtils.canOpenIndex(logger, tempDir, shardId, (id, l, d) -> new DummyShardLock(id))).isTrue();
        Store store = new Store(shardId, INDEX_SETTINGS, dir, new DummyShardLock(shardId));
        store.markStoreCorrupted(new CorruptIndexException("foo", "bar"));
        assertThat(StoreUtils.canOpenIndex(logger, tempDir, shardId, (id, l, d) -> new DummyShardLock(id))).isFalse();
        store.close();
    }

    public void testDeserializeCorruptionException() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        final Directory dir = new ByteBuffersDirectory();
        Store store = new Store(shardId, INDEX_SETTINGS, dir, new DummyShardLock(shardId));
        CorruptIndexException ex = new CorruptIndexException("foo", "bar");
        store.markStoreCorrupted(ex);
        try {
            store.failIfCorrupted();
            fail("should be corrupted");
        } catch (CorruptIndexException e) {
            assertThat(e.getMessage()).isEqualTo(ex.getMessage());
            assertThat(e.toString()).isEqualTo(ex.toString());
            assertStacktraceArrayEquals(ex.getStackTrace(), e.getStackTrace());
        }

        store.removeCorruptionMarker();
        assertThat(store.isMarkedCorrupted()).isFalse();
        FileNotFoundException ioe = new FileNotFoundException("foobar");
        store.markStoreCorrupted(ioe);
        try {
            store.failIfCorrupted();
            fail("should be corrupted");
        } catch (CorruptIndexException e) {
            assertThat(e.getMessage()).isEqualTo("foobar (resource=preexisting_corruption)");
            assertStacktraceArrayEquals(ioe.getStackTrace(), e.getCause().getStackTrace());
        }
        store.close();
    }

    public void testCorruptionMarkerVersionCheck() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        final Directory dir = new ByteBuffersDirectory();

        try (Store store = new Store(shardId, INDEX_SETTINGS, dir, new DummyShardLock(shardId))) {
            final String corruptionMarkerName = Store.CORRUPTED_MARKER_NAME_PREFIX + UUIDs.randomBase64UUID();
            try (IndexOutput output = dir.createOutput(corruptionMarkerName, IOContext.DEFAULT)) {
                CodecUtil.writeHeader(output, Store.CODEC, Store.CORRUPTED_MARKER_CODEC_VERSION + randomFrom(1, 2, -1, -2, -3));
                // we only need the header to trigger the exception
            }
            assertThatThrownBy(store::failIfCorrupted)
                .isInstanceOfAny(IndexFormatTooOldException.class, IndexFormatTooNewException.class)
                .hasMessageContaining(corruptionMarkerName);
        }
    }

    public void testHistoryUUIDCanBeForced() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        try (Store store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), new DummyShardLock(shardId))) {

            store.createEmpty(Version.LATEST);

            SegmentInfos segmentInfos = Lucene.readSegmentInfos(store.directory());
            assertThat(segmentInfos.getUserData(), hasKey(Engine.HISTORY_UUID_KEY));
            final String oldHistoryUUID = segmentInfos.getUserData().get(Engine.HISTORY_UUID_KEY);

            store.bootstrapNewHistory();

            segmentInfos = Lucene.readSegmentInfos(store.directory());
            assertThat(segmentInfos.getUserData(), hasKey(Engine.HISTORY_UUID_KEY));
            assertThat(segmentInfos.getUserData().get(Engine.HISTORY_UUID_KEY)).isNotEqualTo(oldHistoryUUID);
        }
    }

    public void testGetPendingFiles() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        final String testfile = "testfile";
        try (Store store = new Store(shardId, INDEX_SETTINGS, new NIOFSDirectory(createTempDir()), new DummyShardLock(shardId))) {
            store.directory().createOutput(testfile, IOContext.DEFAULT).close();
            try (IndexInput input = store.directory().openInput(testfile, IOContext.DEFAULT)) {
                store.directory().deleteFile(testfile);
                assertThat(store.directory().getPendingDeletions()).isEqualTo(FilterDirectory.unwrap(store.directory()).getPendingDeletions());
            }
        }
    }
}
