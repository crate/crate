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

package io.crate.lucene.codec;

import java.io.IOException;
import java.util.zip.CRC32;

import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;

public class UnbufferedChecksumIndexInput extends ChecksumIndexInput {

    final IndexInput main;
    final CRC32 digest;

    public UnbufferedChecksumIndexInput(IndexInput main) {
        super("BufferedChecksumIndexInput(" + main + ")");
        this.main = main;
        this.digest = new CRC32();
    }

    @Override
    public byte readByte() throws IOException {
        final byte b = main.readByte();
        digest.update(b);
        return b;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        main.readBytes(b, offset, len);
        digest.update(b, offset, len);
    }

    @Override
    public short readShort() throws IOException {
        throw new UnsupportedOperationException("NYI");
    }

    @Override
    public int readInt() throws IOException {
        throw new UnsupportedOperationException("NYI");
    }

    @Override
    public long readLong() throws IOException {
        throw new UnsupportedOperationException("NYI");
    }

    @Override
    public void readLongs(long[] dst, int offset, int length) throws IOException {
        throw new UnsupportedOperationException("NYI");
    }

    @Override
    public long getChecksum() {
        return digest.getValue();
    }

    @Override
    public void close() throws IOException {
        main.close();
    }

    @Override
    public long getFilePointer() {
        return main.getFilePointer();
    }

    @Override
    public long length() {
        return main.length();
    }

    @Override
    public IndexInput clone() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        throw new UnsupportedOperationException();
    }
}

