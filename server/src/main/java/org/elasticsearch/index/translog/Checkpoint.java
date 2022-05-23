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

import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;

final class Checkpoint {

    final long offset;
    final int numOps;
    final long generation;
    final long minSeqNo;
    final long maxSeqNo;
    final long globalCheckpoint;
    final long minTranslogGeneration;
    final long trimmedAboveSeqNo;

    private static final int INITIAL_VERSION = 1; // start with 1, just to recognize there was some magic serialization logic before
    private static final int VERSION_6_0_0 = 2; // introduction of global checkpoints
    private static final int VERSION_6_4_0 = 3; // introduction of trimmed above seq#
    private static final int CURRENT_VERSION = 4; // Lucene 9+ switches to little endian

    private static final String CHECKPOINT_CODEC = "ckp";

    // size of 6.4.0 checkpoint
    static final int V3_FILE_SIZE = CodecUtil.headerLength(CHECKPOINT_CODEC)
        + Integer.BYTES  // ops
        + Long.BYTES // offset
        + Long.BYTES // generation
        + Long.BYTES // minimum sequence number, introduced in 6.0.0
        + Long.BYTES // maximum sequence number, introduced in 6.0.0
        + Long.BYTES // global checkpoint, introduced in 6.0.0
        + Long.BYTES // minimum translog generation in the translog - introduced in 6.0.0
        + Long.BYTES // maximum reachable (trimmed) sequence number, introduced in 6.4.0
        + CodecUtil.footerLength();

    // size of 6.0.0 checkpoint
    static final int V2_FILE_SIZE = CodecUtil.headerLength(CHECKPOINT_CODEC)
        + Integer.BYTES  // ops
        + Long.BYTES // offset
        + Long.BYTES // generation
        + Long.BYTES // minimum sequence number, introduced in 6.0.0
        + Long.BYTES // maximum sequence number, introduced in 6.0.0
        + Long.BYTES // global checkpoint, introduced in 6.0.0
        + Long.BYTES // minimum translog generation in the translog - introduced in 6.0.0
        + CodecUtil.footerLength();

    // size of 5.0.0 checkpoint
    static final int V1_FILE_SIZE = CodecUtil.headerLength(CHECKPOINT_CODEC)
        + Integer.BYTES  // ops
        + Long.BYTES // offset
        + Long.BYTES // generation
        + CodecUtil.footerLength();

    static final int V4_FILE_SIZE = V3_FILE_SIZE;

    /**
     * Create a new translog checkpoint.
     *
     * @param offset                the current offset in the translog
     * @param numOps                the current number of operations in the translog
     * @param generation            the current translog generation
     * @param minSeqNo              the current minimum sequence number of all operations in the translog
     * @param maxSeqNo              the current maximum sequence number of all operations in the translog
     * @param globalCheckpoint      the last-known global checkpoint
     * @param minTranslogGeneration the minimum generation referenced by the translog at this moment.
     * @param trimmedAboveSeqNo     all operations with seq# above trimmedAboveSeqNo should be ignored and not read from the
     *                              corresponding translog file. {@link SequenceNumbers#UNASSIGNED_SEQ_NO} is used to disable trimming.
     */
    Checkpoint(long offset, int numOps, long generation, long minSeqNo, long maxSeqNo, long globalCheckpoint,
               long minTranslogGeneration, long trimmedAboveSeqNo) {
        assert minSeqNo <= maxSeqNo : "minSeqNo [" + minSeqNo + "] is higher than maxSeqNo [" + maxSeqNo + "]";
        assert trimmedAboveSeqNo <= maxSeqNo : "trimmedAboveSeqNo [" + trimmedAboveSeqNo + "] is higher than maxSeqNo [" + maxSeqNo + "]";
        assert minTranslogGeneration <= generation :
            "minTranslogGen [" + minTranslogGeneration + "] is higher than generation [" + generation + "]";
        this.offset = offset;
        this.numOps = numOps;
        this.generation = generation;
        this.minSeqNo = minSeqNo;
        this.maxSeqNo = maxSeqNo;
        this.globalCheckpoint = globalCheckpoint;
        this.minTranslogGeneration = minTranslogGeneration;
        this.trimmedAboveSeqNo = trimmedAboveSeqNo;
    }

    private void write(DataOutput out) throws IOException {
        out.writeLong(offset);
        out.writeInt(numOps);
        out.writeLong(generation);
        out.writeLong(minSeqNo);
        out.writeLong(maxSeqNo);
        out.writeLong(globalCheckpoint);
        out.writeLong(minTranslogGeneration);
        out.writeLong(trimmedAboveSeqNo);
    }

    /**
     * Returns the maximum sequence number of operations in this checkpoint after applying {@link #trimmedAboveSeqNo}.
     */
    long maxEffectiveSeqNo() {
        if (trimmedAboveSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO) {
            return maxSeqNo;
        } else {
            return Math.min(trimmedAboveSeqNo, maxSeqNo);
        }
    }

    static Checkpoint emptyTranslogCheckpoint(final long offset, final long generation, final long globalCheckpoint,
                                              long minTranslogGeneration) {
        final long minSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        final long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        final long trimmedAboveSeqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
        return new Checkpoint(offset, 0, generation, minSeqNo, maxSeqNo, globalCheckpoint, minTranslogGeneration, trimmedAboveSeqNo);
    }

    static Checkpoint readCheckpointV4(final DataInput in) throws IOException {
        final long offset = in.readLong();
        final int numOps = in.readInt();
        final long generation = in.readLong();
        final long minSeqNo = in.readLong();
        final long maxSeqNo = in.readLong();
        final long globalCheckpoint = in.readLong();
        final long minTranslogGeneration = in.readLong();
        final long trimmedAboveSeqNo = in.readLong();
        return new Checkpoint(offset, numOps, generation, minSeqNo, maxSeqNo, globalCheckpoint, minTranslogGeneration, trimmedAboveSeqNo);
    }

    static Checkpoint readCheckpointV6_4_0(final DataInput in) throws IOException {
        // CURRENT_VERSION is equivalent to V_6_4_0(V3) but with the endianness flipped.
        final DataInput reversedIn = EndiannessReverserUtil.wrapDataInput(in);
        final long offset = reversedIn.readLong();
        final int numOps = reversedIn.readInt();
        final long generation = reversedIn.readLong();
        final long minSeqNo = reversedIn.readLong();
        final long maxSeqNo = reversedIn.readLong();
        final long globalCheckpoint = reversedIn.readLong();
        final long minTranslogGeneration = reversedIn.readLong();
        final long trimmedAboveSeqNo = reversedIn.readLong();
        return new Checkpoint(offset, numOps, generation, minSeqNo, maxSeqNo, globalCheckpoint, minTranslogGeneration, trimmedAboveSeqNo);
    }

    static Checkpoint readCheckpointV6_0_0(final DataInput in) throws IOException {
        final DataInput reversedIn = EndiannessReverserUtil.wrapDataInput(in);
        final long offset = reversedIn.readLong();
        final int numOps = reversedIn.readInt();
        final long generation = reversedIn.readLong();
        final long minSeqNo = reversedIn.readLong();
        final long maxSeqNo = reversedIn.readLong();
        final long globalCheckpoint = reversedIn.readLong();
        final long minTranslogGeneration = reversedIn.readLong();
        final long trimmedAboveSeqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
        return new Checkpoint(offset, numOps, generation, minSeqNo, maxSeqNo, globalCheckpoint, minTranslogGeneration, trimmedAboveSeqNo);
    }

    // reads a checksummed checkpoint introduced in ES 5.0.0
    static Checkpoint readCheckpointV5_0_0(final DataInput in) throws IOException {
        final DataInput reversedIn = EndiannessReverserUtil.wrapDataInput(in);
        final long offset = reversedIn.readLong();
        final int numOps = reversedIn.readInt();
        final long generation = reversedIn.readLong();
        final long minSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        final long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        final long globalCheckpoint = SequenceNumbers.UNASSIGNED_SEQ_NO;
        final long minTranslogGeneration = -1;
        final long trimmedAboveSeqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
        return new Checkpoint(offset, numOps, generation, minSeqNo, maxSeqNo, globalCheckpoint, minTranslogGeneration, trimmedAboveSeqNo);
    }

    @Override
    public String toString() {
        return "Checkpoint{" +
            "offset=" + offset +
            ", numOps=" + numOps +
            ", generation=" + generation +
            ", minSeqNo=" + minSeqNo +
            ", maxSeqNo=" + maxSeqNo +
            ", globalCheckpoint=" + globalCheckpoint +
            ", minTranslogGeneration=" + minTranslogGeneration +
            ", trimmedAboveSeqNo=" + trimmedAboveSeqNo +
            '}';
    }

    public static Checkpoint read(Path path) throws IOException {
        try (Directory dir = new NIOFSDirectory(path.getParent())) {
            try (IndexInput indexInput = dir.openInput(path.getFileName().toString(), IOContext.DEFAULT)) {
                // We checksum the entire file before we even go and parse it. If it's corrupted we barf right here.
                CodecUtil.checksumEntireFile(indexInput);
                final int fileVersion = CodecUtil.checkHeader(indexInput, CHECKPOINT_CODEC, INITIAL_VERSION, CURRENT_VERSION);
                if (fileVersion == INITIAL_VERSION) {
                    assert indexInput.length() == V1_FILE_SIZE : indexInput.length();
                    return Checkpoint.readCheckpointV5_0_0(indexInput);
                } else if (fileVersion == VERSION_6_0_0) {
                    assert indexInput.length() == V2_FILE_SIZE : indexInput.length();
                    return Checkpoint.readCheckpointV6_0_0(indexInput);
                } else if (fileVersion == VERSION_6_4_0) {
                    assert indexInput.length() == V3_FILE_SIZE : indexInput.length();
                    return Checkpoint.readCheckpointV6_4_0(indexInput);
                } else {
                    assert fileVersion == CURRENT_VERSION : fileVersion;
                    assert indexInput.length() == V4_FILE_SIZE : indexInput.length();
                    return Checkpoint.readCheckpointV4(indexInput);
                }
            } catch (CorruptIndexException | NoSuchFileException | IndexFormatTooOldException | IndexFormatTooNewException e) {
                throw new TranslogCorruptedException(path.toString(), e);
            }
        }
    }

    public static void write(ChannelFactory factory, Path checkpointFile, Checkpoint checkpoint, OpenOption... options) throws IOException {
        byte[] bytes = createCheckpointBytes(checkpointFile, checkpoint);

        // now go and write to the channel, in one go.
        try (FileChannel channel = factory.open(checkpointFile, options)) {
            Channels.writeToChannel(bytes, channel);
            // force fsync with metadata since this is used on file creation
            channel.force(true);
        }
    }

    public static void write(FileChannel fileChannel, Path checkpointFile, Checkpoint checkpoint) throws IOException {
        byte[] bytes = createCheckpointBytes(checkpointFile, checkpoint);
        Channels.writeToChannel(bytes, fileChannel, 0);
        // no need to force metadata, file size stays the same and we did the full fsync
        // when we first created the file, so the directory entry doesn't change as well
        fileChannel.force(false);
    }

    private static byte[] createCheckpointBytes(Path checkpointFile, Checkpoint checkpoint) throws IOException {
        final ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream(V4_FILE_SIZE) {
            @Override
            public synchronized byte[] toByteArray() {
                // don't clone
                return buf;
            }
        };
        final String resourceDesc = "checkpoint(path=\"" + checkpointFile + "\", gen=" + checkpoint + ")";
        try (OutputStreamIndexOutput indexOutput =
                 new OutputStreamIndexOutput(resourceDesc, checkpointFile.toString(), byteOutputStream, V4_FILE_SIZE)) {
            CodecUtil.writeHeader(indexOutput, CHECKPOINT_CODEC, CURRENT_VERSION);
            checkpoint.write(indexOutput);
            CodecUtil.writeFooter(indexOutput);

            assert indexOutput.getFilePointer() == V4_FILE_SIZE :
                "get you numbers straight; bytes written: " + indexOutput.getFilePointer() + ", buffer size: " + V4_FILE_SIZE;
            assert indexOutput.getFilePointer() < 512 :
                "checkpoint files have to be smaller than 512 bytes for atomic writes; size: " + indexOutput.getFilePointer();
        }
        return byteOutputStream.toByteArray();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Checkpoint that = (Checkpoint) o;

        if (offset != that.offset) return false;
        if (numOps != that.numOps) return false;
        if (generation != that.generation) return false;
        if (minSeqNo != that.minSeqNo) return false;
        if (maxSeqNo != that.maxSeqNo) return false;
        if (globalCheckpoint != that.globalCheckpoint) return false;
        return trimmedAboveSeqNo == that.trimmedAboveSeqNo;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(offset);
        result = 31 * result + numOps;
        result = 31 * result + Long.hashCode(generation);
        result = 31 * result + Long.hashCode(minSeqNo);
        result = 31 * result + Long.hashCode(maxSeqNo);
        result = 31 * result + Long.hashCode(globalCheckpoint);
        result = 31 * result + Long.hashCode(trimmedAboveSeqNo);
        return result;
    }

}
