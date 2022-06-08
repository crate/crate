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

    /** Default constructor. */
    public CustomLucene90DocValuesFormat() {
        this(Mode.BEST_SPEED);
    }

    public CustomLucene90DocValuesFormat(Mode mode) {
        super("CrateDBLucene90");
        this.mode = mode;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new CustomLucene90DocValuesConsumer(
                state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION, mode);
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

}
