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

package org.elasticsearch.common.hash;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;



/**
 * MurmurHash3 hashing functions.
 *
 * See https://code.google.com/p/smhasher/source/browse/trunk/MurmurHash3.cpp#255
 * and https://github.com/prasanthj/hyperloglog/blob/master/src/java/com/github/prasanthj/hll/Murmur3.java
 */
public enum MurmurHash3 {
    ;

    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;
    private static final int R1 = 31;
    private static final int R2 = 27;
    private static final int M = 5;
    private static final int N1 = 0x52dce729;
    private static final int DEFAULT_SEED = 123;
    private static final VarHandle VH_LE_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);


    protected static long fmix(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }

    /**
     * Murmur3 64-bit variant. This is essentially MSB 8 bytes of Murmur3 128-bit variant.
     *
     * @param data   - input byte array
     * @param length - length of array
     * @return - hashcode
     */
    public static long hash64(byte[] data, int offset, int length) {
        return hash64(data, offset, length, DEFAULT_SEED);
    }

    /**
     * Murmur3 64-bit variant. This is essentially MSB 8 bytes of Murmur3 128-bit variant.
     *
     * @param data   - input byte array
     * @param offset - starting position.
     * @param length - length of processed bytes starting from the offset.
     * @param seed   - seed.
     * @return - hashcode
     */
    @SuppressWarnings("fall through")
    public static long hash64(byte[] data, int offset, int length, int seed) {
        long hash = seed;
        final int nblocks = length >> 3;

        // body
        for (int i = 0; i < nblocks; i++) {
            final int i8 = (i << 3) + offset;
            long k = (long) VH_LE_LONG.get(data, i8);

            // mix functions
            k *= C1;
            k = Long.rotateLeft(k, R1);
            k *= C2;
            hash ^= k;
            hash = Long.rotateLeft(hash, R2) * M + N1;
        }

        // tail
        long k1 = 0;
        int tailStart = (nblocks << 3) + offset;
        switch (length - (nblocks << 3)) {
            case 7:
                k1 ^= ((long) data[tailStart + 6] & 0xff) << 48;
                // fall through
            case 6:
                k1 ^= ((long) data[tailStart + 5] & 0xff) << 40;
                // fall through
            case 5:
                k1 ^= ((long) data[tailStart + 4] & 0xff) << 32;
                // fall through
            case 4:
                k1 ^= ((long) data[tailStart + 3] & 0xff) << 24;
                // fall through
            case 3:
                k1 ^= ((long) data[tailStart + 2] & 0xff) << 16;
                // fall through
            case 2:
                k1 ^= ((long) data[tailStart + 1] & 0xff) << 8;
                // fall through
            case 1:
                k1 ^= ((long) data[tailStart] & 0xff);
                k1 *= C1;
                k1 = Long.rotateLeft(k1, R1);
                k1 *= C2;
                hash ^= k1;
                // fall through
            default:
                break;
        }

        // finalization
        hash ^= length;
        hash = fmix(hash);

        return hash;
    }
}
