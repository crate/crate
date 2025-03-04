/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.crate.lucene.codec;

import java.io.IOException;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * Copy of {@link Lucene90DocValuesFormat} that adds a configuration option for termsDict compression.
 */
public final class CustomLucene90DocValuesFormat extends DocValuesFormat {

    public enum Mode {
        BEST_SPEED,
        BEST_COMPRESSION
    }

    private final Mode mode;
    private final int skipIndexIntervalSize;

    /** Default constructor. */
    public CustomLucene90DocValuesFormat() {
        this(Mode.BEST_SPEED, DEFAULT_SKIP_INDEX_INTERVAL_SIZE);
    }

    public CustomLucene90DocValuesFormat(Mode mode) {
        this(mode, DEFAULT_SKIP_INDEX_INTERVAL_SIZE);
    }

    public CustomLucene90DocValuesFormat(Mode mode, int skipIndexIntervalSize) {
        super("CrateDBLucene90");
        this.mode = mode;
        if (skipIndexIntervalSize < 2) {
            throw new IllegalArgumentException(
                "skipIndexIntervalSize must be > 1, got [" + skipIndexIntervalSize + "]");
        }
        this.skipIndexIntervalSize = skipIndexIntervalSize;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new CustomLucene90DocValuesConsumer(
                state, skipIndexIntervalSize, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION, mode);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new CustomLucene90DocValuesProducer(
                state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
    }

    static final String DATA_CODEC = "Lucene90DocValuesData";
    static final String DATA_EXTENSION = "dvd";
    static final String META_CODEC = "Lucene90DocValuesMetadata";
    static final String META_EXTENSION = "dvm";
    static final int VERSION_START = 0;
    static final int VERSION_CURRENT = VERSION_START;

    // indicates docvalues type
    static final byte NUMERIC = 0;
    static final byte BINARY = 1;
    static final byte SORTED = 2;
    static final byte SORTED_SET = 3;
    static final byte SORTED_NUMERIC = 4;

    static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

    static final int NUMERIC_BLOCK_SHIFT = 14;
    static final int NUMERIC_BLOCK_SIZE = 1 << NUMERIC_BLOCK_SHIFT;

    static final int TERMS_DICT_BLOCK_SHIFT = 4;
    static final int TERMS_DICT_BLOCK_SIZE = 1 << TERMS_DICT_BLOCK_SHIFT;
    static final int TERMS_DICT_BLOCK_MASK = TERMS_DICT_BLOCK_SIZE - 1;

    static final int TERMS_DICT_BLOCK_COMPRESSION_THRESHOLD = 32;

    static final int TERMS_DICT_BLOCK_LZ4_SHIFT = 6;
    static final int TERMS_DICT_BLOCK_LZ4_SIZE = 1 << TERMS_DICT_BLOCK_LZ4_SHIFT;
    static final int TERMS_DICT_BLOCK_LZ4_MASK = TERMS_DICT_BLOCK_LZ4_SIZE - 1;

    static final int TERMS_DICT_REVERSE_INDEX_SHIFT = 10;
    static final int TERMS_DICT_REVERSE_INDEX_SIZE = 1 << TERMS_DICT_REVERSE_INDEX_SHIFT;
    static final int TERMS_DICT_REVERSE_INDEX_MASK = TERMS_DICT_REVERSE_INDEX_SIZE - 1;
    static final int TERMS_DICT_COMPRESSOR_LZ4_CODE = 1;
    // Writing a special code so we know this is a LZ4-compressed block.
    static final int TERMS_DICT_BLOCK_LZ4_CODE =
        TERMS_DICT_BLOCK_LZ4_SHIFT << 16 | TERMS_DICT_COMPRESSOR_LZ4_CODE;

    // number of documents in an interval
    private static final int DEFAULT_SKIP_INDEX_INTERVAL_SIZE = 4096;
    // bytes on an interval:
    //   * 1 byte : number of levels
    //   * 16 bytes: min / max value,
    //   * 8 bytes:  min / max docID
    //   * 4 bytes: number of documents
    private static final long SKIP_INDEX_INTERVAL_BYTES = 29L;
    // number of intervals represented as a shift to create a new level, this is 1 << 3 == 8
    // intervals.
    static final int SKIP_INDEX_LEVEL_SHIFT = 3;
    // max number of levels
    // Increasing this number, it increases how much heap we need at index time.
    // we currently need (1 * 8 * 8 * 8)  = 512 accumulators on heap
    static final int SKIP_INDEX_MAX_LEVEL = 4;
    // number of bytes to skip when skipping a level. It does not take into account the
    // current interval that is being read.
    static final long[] SKIP_INDEX_JUMP_LENGTH_PER_LEVEL = new long[SKIP_INDEX_MAX_LEVEL];

    static {
        // Size of the interval minus read bytes (1 byte for level and 4 bytes for maxDocID)
        SKIP_INDEX_JUMP_LENGTH_PER_LEVEL[0] = SKIP_INDEX_INTERVAL_BYTES - 5L;
        for (int level = 1; level < SKIP_INDEX_MAX_LEVEL; level++) {
            // jump from previous level
            SKIP_INDEX_JUMP_LENGTH_PER_LEVEL[level] = SKIP_INDEX_JUMP_LENGTH_PER_LEVEL[level - 1];
            // nodes added by new level
            SKIP_INDEX_JUMP_LENGTH_PER_LEVEL[level] +=
                (1 << (level * SKIP_INDEX_LEVEL_SHIFT)) * SKIP_INDEX_INTERVAL_BYTES;
            // remove the byte levels added in the previous level
            SKIP_INDEX_JUMP_LENGTH_PER_LEVEL[level] -= (1 << ((level - 1) * SKIP_INDEX_LEVEL_SHIFT));
        }
    }

}
