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

import java.io.IOException;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.ByteArray;

public class PagedBytesReferenceTests extends AbstractBytesReferenceTestCase {

    @Override
    protected BytesReference newBytesReference(int length) throws IOException {
        return newBytesReferenceWithOffsetOfZero(length);
    }

    @Override
    protected BytesReference newBytesReferenceWithOffsetOfZero(int length) throws IOException {
        ByteArray byteArray = bigarrays.newByteArray(length);
        for (int i = 0; i < length; i++) {
            byteArray.set(i, (byte) random().nextInt(1 << 8));
        }
        assertThat(byteArray.size()).isEqualTo(length);
        BytesReference ref = BytesReference.fromByteArray(byteArray, length);
        assertThat(ref.length()).isEqualTo(length);
        if (byteArray.hasArray()) {
            assertThat(ref).isExactlyInstanceOf(BytesArray.class);
        } else {
            assertThat(ref).isExactlyInstanceOf(PagedBytesReference.class);
        }
        return ref;
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
        assertThat(ba).isNotNull();
        assertThat(ba2).isNotNull();
        assertThat(ba.length()).isEqualTo(pbr.length());
        assertThat(ba2.length()).isEqualTo(ba.length());
        // ensure no single-page optimization
        assertThat(ba2.array()).isNotSameAs(ba.array());
    }

    public void testSinglePage() throws IOException {
        int[] sizes = {0, randomInt(PAGE_SIZE), PAGE_SIZE, randomIntBetween(2, PAGE_SIZE * randomIntBetween(2, 5))};

        for (int i = 0; i < sizes.length; i++) {
            BytesReference pbr = newBytesReference(sizes[i]);
            // verify that array() is cheap for small payloads
            if (sizes[i] <= PAGE_SIZE) {
                BytesRef page = getSinglePageOrNull(pbr);
                assertThat(page).isNotNull();
                byte[] array = page.bytes;
                assertThat(array).isNotNull();
                assertThat(array.length).isEqualTo(sizes[i]);
                assertThat(array).isSameAs(page.bytes);
            } else {
                BytesRef page = getSinglePageOrNull(pbr);
                if (pbr.length() > 0) {
                    assertThat(page).isNull();
                }
            }
        }
    }

    public void testToBytes() throws IOException {
        int[] sizes = {0, randomInt(PAGE_SIZE), PAGE_SIZE, randomIntBetween(2, PAGE_SIZE * randomIntBetween(2, 5))};

        for (int i = 0; i < sizes.length; i++) {
            BytesReference pbr = newBytesReference(sizes[i]);
            byte[] bytes = BytesReference.toBytes(pbr);
            assertThat(bytes.length).isEqualTo(sizes[i]);
            // verify that toBytes() is cheap for small payloads
            if (sizes[i] <= PAGE_SIZE) {
                assertThat(bytes).isSameAs(BytesReference.toBytes(pbr));
            } else {
                assertThat(bytes).isNotSameAs(BytesReference.toBytes(pbr));
            }
        }
    }

    public void testHasSinglePage() throws IOException {
        int length = randomIntBetween(10, PAGE_SIZE * randomIntBetween(1, 3));
        BytesReference pbr = newBytesReference(length);
        // must return true for <= pagesize
        assertThat(getNumPages(pbr) == 1).isEqualTo(length <= PAGE_SIZE);
    }

    public void testEquals() {
        int length = randomIntBetween(100, PAGE_SIZE * randomIntBetween(2, 5));
        ByteArray ba1 = bigarrays.newByteArray(length, false);
        ByteArray ba2 = bigarrays.newByteArray(length, false);

        // copy contents
        for (long i = 0; i < length; i++) {
            ba2.set(i, ba1.get(i));
        }

        // get refs & compare
        BytesReference pbr = BytesReference.fromByteArray(ba1, length);
        BytesReference pbr2 = BytesReference.fromByteArray(ba2, length);
        assertThat(pbr2).isEqualTo(pbr);
        int offsetToFlip = randomIntBetween(0, length - 1);
        int value = ~Byte.toUnsignedInt(ba1.get(offsetToFlip));
        ba2.set(offsetToFlip, (byte)value);
        assertThat(pbr2).isNotEqualTo(pbr);
    }
}
