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

package org.elasticsearch.common.bytes;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

public abstract class AbstractBytesReferenceTestCase extends ESTestCase {

    protected static final int PAGE_SIZE = PageCacheRecycler.BYTE_PAGE_SIZE;
    protected final BigArrays bigarrays = new BigArrays(null, new NoneCircuitBreakerService(), CircuitBreaker.REQUEST);

    public void testGet() throws IOException {
        int length = randomIntBetween(1, PAGE_SIZE * 3);
        BytesReference pbr = newBytesReference(length);

        int sliceOffset = randomIntBetween(0, length / 2);
        int sliceLength = Math.max(1, length - sliceOffset - 1);
        BytesReference slice = pbr.slice(sliceOffset, sliceLength);
        assertThat(slice.get(0)).isEqualTo(pbr.get(sliceOffset));
        assertThat(slice.get(sliceLength - 1)).isEqualTo(pbr.get(sliceOffset + sliceLength - 1));
        final int probes = randomIntBetween(20, 100);
        BytesReference copy = new BytesArray(pbr.toBytesRef(), true);
        for (int i = 0; i < probes; i++) {
            int index = randomIntBetween(0, copy.length() - 1);
            assertThat(copy.get(index)).isEqualTo(pbr.get(index));
            index = randomIntBetween(sliceOffset, sliceOffset + sliceLength - 1);
            assertThat(slice.get(index - sliceOffset)).isEqualTo(pbr.get(index));
        }
    }

    public void testLength() throws IOException {
        int[] sizes = {0, randomInt(PAGE_SIZE), PAGE_SIZE, randomInt(PAGE_SIZE * 3)};

        for (int i = 0; i < sizes.length; i++) {
            BytesReference pbr = newBytesReference(sizes[i]);
            assertThat(pbr.length()).isEqualTo(sizes[i]);
        }
    }

    public void testSlice() throws IOException {
        for (int length : new int[] {0, 1, randomIntBetween(2, PAGE_SIZE), randomIntBetween(PAGE_SIZE + 1, 3 * PAGE_SIZE)}) {
            BytesReference pbr = newBytesReference(length);
            int sliceOffset = randomIntBetween(0, length / 2);
            int sliceLength = Math.max(0, length - sliceOffset - 1);
            BytesReference slice = pbr.slice(sliceOffset, sliceLength);
            assertThat(slice.length()).isEqualTo(sliceLength);
            for (int i = 0; i < sliceLength; i++) {
                assertThat(slice.get(i)).isEqualTo(pbr.get(i+sliceOffset));
            }
            BytesRef singlePageOrNull = getSinglePageOrNull(slice);
            if (singlePageOrNull != null) {
                // we can't assert the offset since if the length is smaller than the refercence
                // the offset can be anywhere
                assertThat(singlePageOrNull.length).isEqualTo(sliceLength);
            }
        }
    }

    public void testStreamInput() throws IOException {
        int length = randomIntBetween(10, scaledRandomIntBetween(PAGE_SIZE * 2, PAGE_SIZE * 20));
        BytesReference pbr = newBytesReference(length);
        StreamInput si = pbr.streamInput();
        assertNotNull(si);

        // read single bytes one by one
        assertThat(si.readByte()).isEqualTo(pbr.get(0));
        assertThat(si.readByte()).isEqualTo(pbr.get(1));
        assertThat(si.readByte()).isEqualTo(pbr.get(2));

        // reset the stream for bulk reading
        si.reset();

        // buffer for bulk reads
        byte[] origBuf = new byte[length];
        random().nextBytes(origBuf);
        byte[] targetBuf = Arrays.copyOf(origBuf, origBuf.length);

        // bulk-read 0 bytes: must not modify buffer
        si.readBytes(targetBuf, 0, 0);
        assertThat(targetBuf[0]).isEqualTo(origBuf[0]);
        si.reset();

        // read a few few bytes as ints
        int bytesToRead = randomIntBetween(1, length / 2);
        for (int i = 0; i < bytesToRead; i++) {
            int b = si.read();
            assertThat(b).isEqualTo(pbr.get(i) & 0xff);
        }
        si.reset();

        // bulk-read all
        si.readFully(targetBuf);
        assertArrayEquals(BytesReference.toBytes(pbr), targetBuf);

        // continuing to read should now fail with EOFException
        try {
            si.readByte();
            fail("expected EOF");
        } catch (EOFException | IndexOutOfBoundsException eof) {
            // yay
        }

        // try to read more than the stream contains
        si.reset();
        assertThatThrownBy(() -> si.readBytes(targetBuf, 0, length * 2))
            .isExactlyInstanceOf(IndexOutOfBoundsException.class);
    }

    public void testStreamInputMarkAndReset() throws IOException {
        int length = randomIntBetween(10, scaledRandomIntBetween(PAGE_SIZE * 2, PAGE_SIZE * 20));
        BytesReference pbr = newBytesReference(length);
        StreamInput si = pbr.streamInput();
        assertNotNull(si);

        StreamInput wrap = StreamInput.wrap(BytesReference.toBytes(pbr));
        while(wrap.available() > 0) {
            if (rarely()) {
                wrap.mark(Integer.MAX_VALUE);
                si.mark(Integer.MAX_VALUE);
            } else if (rarely()) {
                wrap.reset();
                si.reset();
            }
            assertThat(wrap.readByte()).isEqualTo(si.readByte());
            assertThat(wrap.available()).isEqualTo(si.available());
        }
    }

    public void testStreamInputBulkReadWithOffset() throws IOException {
        final int length = randomIntBetween(10, scaledRandomIntBetween(PAGE_SIZE * 2, PAGE_SIZE * 20));
        BytesReference pbr = newBytesReference(length);
        StreamInput si = pbr.streamInput();
        assertNotNull(si);

        // read a bunch of single bytes one by one
        int offset = randomIntBetween(1, length / 2);
        for (int i = 0; i < offset; i++) {
            assertThat(length - i).isEqualTo(si.available());
            assertThat(si.readByte()).isEqualTo(pbr.get(i));
        }

        // now do NOT reset the stream - keep the stream's offset!

        // buffer to compare remaining bytes against bulk read
        byte[] pbrBytesWithOffset = Arrays.copyOfRange(BytesReference.toBytes(pbr), offset, length);
        // randomized target buffer to ensure no stale slots
        byte[] targetBytes = new byte[pbrBytesWithOffset.length];
        random().nextBytes(targetBytes);

        // bulk-read all
        si.readFully(targetBytes);
        assertArrayEquals(pbrBytesWithOffset, targetBytes);
        assertThat(0).isEqualTo(si.available());
    }

    public void testRandomReads() throws IOException {
        int length = randomIntBetween(10, scaledRandomIntBetween(PAGE_SIZE * 2, PAGE_SIZE * 20));
        BytesReference pbr = newBytesReference(length);
        StreamInput streamInput = pbr.streamInput();
        BytesRefBuilder target = new BytesRefBuilder();
        while (target.length() < pbr.length()) {
            switch (randomIntBetween(0, 10)) {
                case 6:
                case 5:
                    target.append(new BytesRef(new byte[]{streamInput.readByte()}));
                    break;
                case 4:
                case 3:
                    BytesRef bytesRef = streamInput.readBytesRef(scaledRandomIntBetween(1, pbr.length() - target.length()));
                    target.append(bytesRef);
                    break;
                default:
                    byte[] buffer = new byte[scaledRandomIntBetween(1, pbr.length() - target.length())];
                    int offset = scaledRandomIntBetween(0, buffer.length - 1);
                    int read = streamInput.read(buffer, offset, buffer.length - offset);
                    target.append(new BytesRef(buffer, offset, read));
                    break;
            }
        }
        assertThat(target.length()).isEqualTo(pbr.length());
        BytesRef targetBytes = target.get();
        assertArrayEquals(BytesReference.toBytes(pbr), Arrays.copyOfRange(targetBytes.bytes, targetBytes.offset, targetBytes.length));
    }

    public void testSliceStreamInput() throws IOException {
        int length = randomIntBetween(10, scaledRandomIntBetween(PAGE_SIZE * 2, PAGE_SIZE * 20));
        BytesReference pbr = newBytesReference(length);

        // test stream input over slice (upper half of original)
        int sliceOffset = randomIntBetween(1, length / 2);
        int sliceLength = length - sliceOffset;
        BytesReference slice = pbr.slice(sliceOffset, sliceLength);
        StreamInput sliceInput = slice.streamInput();
        assertThat(sliceLength).isEqualTo(sliceInput.available());

        // single reads
        assertThat(sliceInput.readByte()).isEqualTo(slice.get(0));
        assertThat(sliceInput.readByte()).isEqualTo(slice.get(1));
        assertThat(sliceInput.readByte()).isEqualTo(slice.get(2));
        assertThat(sliceLength - 3).isEqualTo(sliceInput.available());

        // reset the slice stream for bulk reading
        sliceInput.reset();
        assertThat(sliceLength).isEqualTo(sliceInput.available());

        // bulk read
        byte[] sliceBytes = new byte[sliceLength];
        sliceInput.readFully(sliceBytes);
        assertThat(0).isEqualTo(sliceInput.available());

        // compare slice content with upper half of original
        byte[] pbrSliceBytes = Arrays.copyOfRange(BytesReference.toBytes(pbr), sliceOffset, length);
        assertArrayEquals(pbrSliceBytes, sliceBytes);

        // compare slice bytes with bytes read from slice via streamInput :D
        byte[] sliceToBytes = BytesReference.toBytes(slice);
        assertThat(sliceToBytes.length).isEqualTo(sliceBytes.length);
        assertArrayEquals(sliceBytes, sliceToBytes);

        sliceInput.reset();
        assertThat(sliceLength).isEqualTo(sliceInput.available());
        byte[] buffer = new byte[sliceLength + scaledRandomIntBetween(1, 100)];
        int offset = scaledRandomIntBetween(0, Math.max(1, buffer.length - sliceLength - 1));
        int read = sliceInput.read(buffer, offset, sliceLength / 2);
        assertThat(sliceLength - read).isEqualTo(sliceInput.available());
        sliceInput.read(buffer, offset + read, sliceLength - read);
        assertArrayEquals(sliceBytes, Arrays.copyOfRange(buffer, offset, offset + sliceLength));
        assertThat(0).isEqualTo(sliceInput.available());
    }

    public void testWriteToOutputStream() throws IOException {
        int length = randomIntBetween(10, PAGE_SIZE * 4);
        BytesReference pbr = newBytesReference(length);
        BytesStreamOutput out = new BytesStreamOutput();
        pbr.writeTo(out);
        assertThat(out.size()).isEqualTo(pbr.length());
        assertArrayEquals(BytesReference.toBytes(pbr), BytesReference.toBytes(out.bytes()));
        out.close();
    }

    public void testInputStreamSkip() throws IOException {
        int length = randomIntBetween(10, scaledRandomIntBetween(PAGE_SIZE * 2, PAGE_SIZE * 20));
        BytesReference pbr = newBytesReference(length);
        final int iters = randomIntBetween(5, 50);
        for (int i = 0; i < iters; i++) {
            try (StreamInput input = pbr.streamInput()) {
                final int offset = randomIntBetween(0, length-1);
                assertThat(input.skip(offset)).isEqualTo(offset);
                assertThat(input.readByte()).isEqualTo(pbr.get(offset));
                if (offset == length - 1) {
                    continue; // no more bytes to retrieve!
                }
                final int nextOffset = randomIntBetween(offset, length-2);
                assertThat(input.skip(nextOffset - offset)).isEqualTo(nextOffset - offset);
                assertThat(input.readByte()).isEqualTo(pbr.get(nextOffset+1)); // +1 for the one byte we read above
                assertThat(input.skip(Long.MAX_VALUE)).isEqualTo(length - (nextOffset+2));
                assertThat(input.skip(randomIntBetween(0, Integer.MAX_VALUE))).isEqualTo(0);
            }
        }
    }

    public void testSliceWriteToOutputStream() throws IOException {
        int length = randomIntBetween(10, PAGE_SIZE * randomIntBetween(2, 5));
        BytesReference pbr = newBytesReference(length);
        int sliceOffset = randomIntBetween(1, length / 2);
        int sliceLength = length - sliceOffset;
        BytesReference slice = pbr.slice(sliceOffset, sliceLength);
        BytesStreamOutput sliceOut = new BytesStreamOutput(sliceLength);
        slice.writeTo(sliceOut);
        assertThat(sliceOut.size()).isEqualTo(slice.length());
        assertArrayEquals(BytesReference.toBytes(slice), BytesReference.toBytes(sliceOut.bytes()));
        sliceOut.close();
    }

    public void testToBytes() throws IOException {
        int[] sizes = {0, randomInt(PAGE_SIZE), PAGE_SIZE, randomIntBetween(2, PAGE_SIZE * randomIntBetween(2, 5))};
        for (int i = 0; i < sizes.length; i++) {
            BytesReference pbr = newBytesReference(sizes[i]);
            byte[] bytes = BytesReference.toBytes(pbr);
            assertThat(bytes.length).isEqualTo(sizes[i]);
            for (int j = 0; j  < bytes.length; j++) {
                assertThat(pbr.get(j)).isEqualTo(bytes[j]);
            }
        }
    }

    public void testToBytesRefSharedPage() throws IOException {
        int length = randomIntBetween(10, PAGE_SIZE);
        BytesReference pbr = newBytesReference(length);
        BytesArray ba = new BytesArray(pbr.toBytesRef());
        BytesArray ba2 = new BytesArray(pbr.toBytesRef());
        assertNotNull(ba);
        assertNotNull(ba2);
        assertThat(ba.length()).isEqualTo(pbr.length());
        assertThat(ba2.length()).isEqualTo(ba.length());
        // single-page optimization
        assertSame(ba.array(), ba2.array());
    }

    public void testToBytesRefMaterializedPages() throws IOException {
        // we need a length != (n * pagesize) to avoid page sharing at boundaries
        int length = 0;
        while ((length % PAGE_SIZE) == 0) {
            length = randomIntBetween(PAGE_SIZE, PAGE_SIZE * randomIntBetween(2, 5));
        }
        BytesReference pbr = newBytesReference(length);
        BytesArray ba = new BytesArray(pbr.toBytesRef());
        BytesArray ba2 = new BytesArray(pbr.toBytesRef());
        assertNotNull(ba);
        assertNotNull(ba2);
        assertThat(ba.length()).isEqualTo(pbr.length());
        assertThat(ba2.length()).isEqualTo(ba.length());
    }

    public void testCopyBytesRefSharesBytes() throws IOException {
        // small PBR which would normally share the first page
        int length = randomIntBetween(10, PAGE_SIZE);
        BytesReference pbr = newBytesReference(length);
        BytesArray ba = new BytesArray(pbr.toBytesRef(), true);
        BytesArray ba2 = new BytesArray(pbr.toBytesRef(), true);
        assertNotNull(ba);
        assertNotSame(ba, ba2);
        assertNotSame(ba.array(), ba2.array());
    }

    public void testSliceCopyBytesRef() throws IOException {
        int length = randomIntBetween(10, PAGE_SIZE * randomIntBetween(2, 8));
        BytesReference pbr = newBytesReference(length);
        int sliceOffset = randomIntBetween(0, pbr.length());
        int sliceLength = randomIntBetween(0, pbr.length() - sliceOffset);
        BytesReference slice = pbr.slice(sliceOffset, sliceLength);

        BytesArray ba1 = new BytesArray(slice.toBytesRef(), true);
        BytesArray ba2 = new BytesArray(slice.toBytesRef(), true);
        assertNotNull(ba1);
        assertNotNull(ba2);
        assertNotSame(ba1.array(), ba2.array());
        assertArrayEquals(BytesReference.toBytes(slice), ba1.array());
        assertArrayEquals(BytesReference.toBytes(slice), ba2.array());
        assertArrayEquals(ba1.array(), ba2.array());
    }

    public void testEmptyToBytesRefIterator() throws IOException {
        BytesReference pbr = newBytesReference(0);
        assertNull(pbr.iterator().next());
    }

    public void testIterator() throws IOException {
        int length = randomIntBetween(10, PAGE_SIZE * randomIntBetween(2, 8));
        BytesReference pbr = newBytesReference(length);
        BytesRefIterator iterator = pbr.iterator();
        BytesRef ref;
        BytesRefBuilder builder = new BytesRefBuilder();
        while((ref = iterator.next()) != null) {
            builder.append(ref);
        }
        assertArrayEquals(BytesReference.toBytes(pbr), BytesRef.deepCopyOf(builder.toBytesRef()).bytes);
    }

    public void testSliceIterator() throws IOException {
        int length = randomIntBetween(10, PAGE_SIZE * randomIntBetween(2, 8));
        BytesReference pbr = newBytesReference(length);
        int sliceOffset = randomIntBetween(0, pbr.length());
        int sliceLength = randomIntBetween(0, pbr.length() - sliceOffset);
        BytesReference slice = pbr.slice(sliceOffset, sliceLength);
        BytesRefIterator iterator = slice.iterator();
        BytesRef ref = null;
        BytesRefBuilder builder = new BytesRefBuilder();
        while((ref = iterator.next()) != null) {
            builder.append(ref);
        }
        assertArrayEquals(BytesReference.toBytes(slice), BytesRef.deepCopyOf(builder.toBytesRef()).bytes);
    }

    public void testIteratorRandom() throws IOException {
        int length = randomIntBetween(10, PAGE_SIZE * randomIntBetween(2, 8));
        BytesReference pbr = newBytesReference(length);
        if (randomBoolean()) {
            int sliceOffset = randomIntBetween(0, pbr.length());
            int sliceLength = randomIntBetween(0, pbr.length() - sliceOffset);
            pbr = pbr.slice(sliceOffset, sliceLength);
        }

        if (randomBoolean()) {
            pbr = new BytesArray(pbr.toBytesRef());
        }
        BytesRefIterator iterator = pbr.iterator();
        BytesRef ref = null;
        BytesRefBuilder builder = new BytesRefBuilder();
        while((ref = iterator.next()) != null) {
            builder.append(ref);
        }
        assertArrayEquals(BytesReference.toBytes(pbr), BytesRef.deepCopyOf(builder.toBytesRef()).bytes);
    }

    public void testArrayOffset() throws IOException {
        int length = randomInt(PAGE_SIZE * randomIntBetween(2, 5));
        BytesReference pbr = newBytesReference(length);
        BytesRef singlePageOrNull = getSinglePageOrNull(pbr);
        if (singlePageOrNull != null) {
            assertThat(singlePageOrNull.offset).isEqualTo(0);
        }
    }

    public void testSliceArrayOffset() throws IOException {
        int length = randomIntBetween(1, PAGE_SIZE * randomIntBetween(2, 5));
        BytesReference pbr = newBytesReferenceWithOffsetOfZero(length);
        int sliceOffset = randomIntBetween(0, pbr.length() - 1); // an offset to the end would be len 0
        int sliceLength = randomIntBetween(1, pbr.length() - sliceOffset);
        BytesReference slice = pbr.slice(sliceOffset, sliceLength);
        BytesRef singlePageOrNull = getSinglePageOrNull(slice);
        if (singlePageOrNull != null) {
            if (getSinglePageOrNull(pbr) == null) {
                // original reference has pages
                assertThat(singlePageOrNull.offset).isEqualTo(sliceOffset % PAGE_SIZE);
            } else {
                // orig ref has no pages ie. BytesArray
                assertThat(singlePageOrNull.offset).isEqualTo(sliceOffset);
            }
        }
    }

    public void testToUtf8() throws IOException {
        // test empty
        BytesReference pbr = newBytesReference(0);
        assertThat(pbr.utf8ToString()).isEqualTo("");
        // TODO: good way to test?
    }

    public void testToBytesRef() throws IOException {
        int length = randomIntBetween(0, PAGE_SIZE);
        BytesReference pbr = newBytesReference(length);
        BytesRef ref = pbr.toBytesRef();
        assertNotNull(ref);
        assertThat(ref.length).isEqualTo(pbr.length());
    }

    public void testSliceToBytesRef() throws IOException {
        int length = randomIntBetween(0, PAGE_SIZE);
        BytesReference pbr = newBytesReferenceWithOffsetOfZero(length);
        // get a BytesRef from a slice
        int sliceOffset = randomIntBetween(0, pbr.length());
        int sliceLength = randomIntBetween(0, pbr.length() - sliceOffset);

        BytesRef sliceRef = pbr.slice(sliceOffset, sliceLength).toBytesRef();

        if (sliceLength == 0 && sliceOffset != sliceRef.offset) {
            // some impls optimize this to an empty instance then the offset will be 0
            assertThat(sliceRef.offset).isEqualTo(0);
        } else {
            // note that these are only true if we have <= than a page, otherwise offset/length are shifted
            assertThat(sliceRef.offset).isEqualTo(sliceOffset);
        }
        assertThat(sliceRef.length).isEqualTo(sliceLength);
    }

    public void testHashCode() throws IOException {
        // empty content must have hash 1 (JDK compat)
        BytesReference pbr = newBytesReference(0);
        assertThat(pbr.hashCode()).isEqualTo(Arrays.hashCode(BytesRef.EMPTY_BYTES));

        // test with content
        pbr = newBytesReference(randomIntBetween(0, PAGE_SIZE * randomIntBetween(2, 5)));
        int jdkHash = Arrays.hashCode(BytesReference.toBytes(pbr));
        int pbrHash = pbr.hashCode();
        assertThat(pbrHash).isEqualTo(jdkHash);

        // test hashes of slices
        int sliceFrom = randomIntBetween(0, pbr.length());
        int sliceLength = randomIntBetween(0, pbr.length() - sliceFrom);
        BytesReference slice = pbr.slice(sliceFrom, sliceLength);
        int sliceJdkHash = Arrays.hashCode(BytesReference.toBytes(slice));
        int sliceHash = slice.hashCode();
        assertThat(sliceHash).isEqualTo(sliceJdkHash);
    }

    public void testEquals() throws IOException {
        BytesReference bytesReference = newBytesReference(randomIntBetween(100, PAGE_SIZE * randomIntBetween(2, 5)));
        BytesReference copy = bytesReference.slice(0, bytesReference.length());

        // get refs & compare
        assertThat(bytesReference).isEqualTo(copy);
        int sliceFrom = randomIntBetween(0, bytesReference.length());
        int sliceLength = randomIntBetween(0, bytesReference.length() - sliceFrom);
        assertThat(bytesReference.slice(sliceFrom, sliceLength)).isEqualTo(copy.slice(sliceFrom, sliceLength));

        BytesRef bytesRef = BytesRef.deepCopyOf(copy.toBytesRef());
        assertThat(copy).isEqualTo(new BytesArray(bytesRef));

        int offsetToFlip = randomIntBetween(0, bytesRef.length - 1);
        int value = ~Byte.toUnsignedInt(bytesRef.bytes[bytesRef.offset+offsetToFlip]);
        bytesRef.bytes[bytesRef.offset+offsetToFlip] = (byte)value;
        assertNotEquals(new BytesArray(bytesRef), copy);
    }

    public void testSliceEquals() {
        int length = randomIntBetween(100, PAGE_SIZE * randomIntBetween(2, 5));
        ByteArray ba1 = bigarrays.newByteArray(length, false);
        BytesReference pbr = BytesReference.fromByteArray(ba1, length);

        // test equality of slices
        int sliceFrom = randomIntBetween(0, pbr.length());
        int sliceLength = randomIntBetween(0, pbr.length() - sliceFrom);
        BytesReference slice1 = pbr.slice(sliceFrom, sliceLength);
        BytesReference slice2 = pbr.slice(sliceFrom, sliceLength);
        assertArrayEquals(BytesReference.toBytes(slice1), BytesReference.toBytes(slice2));

        // test a slice with same offset but different length,
        // unless randomized testing gave us a 0-length slice.
        if (sliceLength > 0) {
            BytesReference slice3 = pbr.slice(sliceFrom, sliceLength / 2);
            assertThat(Arrays.equals(BytesReference.toBytes(slice1), BytesReference.toBytes(slice3))).isFalse();
        }
    }

    protected abstract BytesReference newBytesReference(int length) throws IOException;

    protected abstract BytesReference newBytesReferenceWithOffsetOfZero(int length) throws IOException;

    public void testCompareTo() throws IOException {
        final int iters = randomIntBetween(5, 10);
        for (int i = 0; i < iters; i++) {
            int length = randomIntBetween(10, PAGE_SIZE * randomIntBetween(2, 8));
            BytesReference bytesReference = newBytesReference(length);
            assertThat(bytesReference.compareTo(new BytesArray("")) > 0).isTrue();
            assertThat(new BytesArray("").compareTo(bytesReference) < 0).isTrue();


            assertThat(bytesReference.compareTo(bytesReference)).isEqualTo(0);
            int sliceFrom = randomIntBetween(0, bytesReference.length());
            int sliceLength = randomIntBetween(0, bytesReference.length() - sliceFrom);
            BytesReference slice = bytesReference.slice(sliceFrom, sliceLength);

            assertThat(new BytesArray(bytesReference.toBytesRef(), true).compareTo(new BytesArray(slice.toBytesRef(), true))).isEqualTo(bytesReference.toBytesRef().compareTo(slice.toBytesRef()));

            assertThat(bytesReference.compareTo(slice)).isEqualTo(bytesReference.toBytesRef().compareTo(slice.toBytesRef()));
            assertThat(slice.compareTo(bytesReference)).isEqualTo(slice.toBytesRef().compareTo(bytesReference.toBytesRef()));

            assertThat(slice.compareTo(new BytesArray(slice.toBytesRef()))).isEqualTo(0);
            assertThat(new BytesArray(slice.toBytesRef()).compareTo(slice)).isEqualTo(0);

            final int crazyLength = length + randomIntBetween(10, PAGE_SIZE * randomIntBetween(2, 8));
            ReleasableBytesStreamOutput crazyStream = new ReleasableBytesStreamOutput(length, bigarrays);
            final int offset = randomIntBetween(0, crazyLength - length);
            for (int j = 0; j < offset; j++) {
                crazyStream.writeByte((byte) random().nextInt(1 << 8));
            }
            bytesReference.writeTo(crazyStream);
            for (int j = crazyStream.size(); j < crazyLength; j++) {
                crazyStream.writeByte((byte) random().nextInt(1 << 8));
            }
            BytesReference crazyReference = crazyStream.bytes();

            assertThat(crazyReference.compareTo(bytesReference) == 0).isFalse();
            assertThat(crazyReference.slice(offset, length).compareTo(
                bytesReference)).isEqualTo(0);
            assertThat(bytesReference.compareTo(
                crazyReference.slice(offset, length))).isEqualTo(0);
        }
    }

    public static BytesRef getSinglePageOrNull(BytesReference ref) throws IOException {
        if (ref.length() > 0) {
            BytesRefIterator iterator = ref.iterator();
            BytesRef next = iterator.next();
            BytesRef retVal = next.clone();
            if (iterator.next() == null) {
                return retVal;
            }
        } else {
            return new BytesRef();
        }
        return null;
    }

    public static int getNumPages(BytesReference ref) throws IOException {
        int num = 0;
        if (ref.length() > 0) {
            BytesRefIterator iterator = ref.iterator();
            while(iterator.next() != null) {
                num++;
            }
        }
        return num;
    }


    public void testBasicEquals() {
        final int len = randomIntBetween(0, randomBoolean() ? 10: 100000);
        final int offset1 = randomInt(5);
        final byte[] array1 = new byte[offset1 + len + randomInt(5)];
        random().nextBytes(array1);
        final int offset2 = randomInt(offset1);
        final byte[] array2 = Arrays.copyOfRange(array1, offset1 - offset2, array1.length);

        final BytesArray b1 = new BytesArray(array1, offset1, len);
        final BytesArray b2 = new BytesArray(array2, offset2, len);
        assertThat(b2).isEqualTo(b1);
        assertThat(b1.hashCode()).isEqualTo(Arrays.hashCode(BytesReference.toBytes(b1)));
        assertThat(b2.hashCode()).isEqualTo(Arrays.hashCode(BytesReference.toBytes(b2)));

        // test same instance
        assertThat(b1).isEqualTo(b1);
        assertThat(b2).isEqualTo(b2);

        if (len > 0) {
            // test different length
            BytesArray differentLen = new BytesArray(array1, offset1, randomInt(len - 1));
            assertNotEquals(b1, differentLen);

            // test changed bytes
            array1[offset1 + randomInt(len - 1)] += 13;
            assertNotEquals(b1, b2);
        }
    }

    public void testGetInt() throws IOException {
        final int count = randomIntBetween(1, 10);
        final BytesReference bytesReference = newBytesReference(count * Integer.BYTES);
        final BytesRef bytesRef = bytesReference.toBytesRef();
        final IntBuffer intBuffer =
            ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length).order(ByteOrder.BIG_ENDIAN).asIntBuffer();
        for (int i = 0; i < count; ++i) {
            assertThat(bytesReference.getInt(i * Integer.BYTES)).isEqualTo(intBuffer.get(i));
        }
    }

    public void testIndexOf() throws IOException {
        final int size = randomIntBetween(0, 100);
        final BytesReference bytesReference = newBytesReference(size);
        final Map<Byte, List<Integer>> map = new HashMap<>();
        for (int i = 0; i < size; ++i) {
            final byte value = bytesReference.get(i);
            map.computeIfAbsent(value, v -> new ArrayList<>()).add(i);
        }
        map.forEach((value, positions) -> {
            for (int i = 0; i < positions.size(); i++) {
                final int pos = positions.get(i);
                final int from = i == 0 ? randomIntBetween(0, pos) : positions.get(i - 1) + 1;
                assertThat(pos).isEqualTo(bytesReference.indexOf(value, from));
            }
        });
        final byte missing = randomValueOtherThanMany(map::containsKey, ESTestCase::randomByte);
        assertThat(bytesReference.indexOf(missing, randomIntBetween(0, Math.max(0, size - 1)))).isEqualTo(-1);
    }
}
