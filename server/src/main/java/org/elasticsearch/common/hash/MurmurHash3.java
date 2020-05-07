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

import org.elasticsearch.common.util.ByteUtils;


/**
 * MurmurHash3 hashing functions.
 *
 * See https://code.google.com/p/smhasher/source/browse/trunk/MurmurHash3.cpp#255
 * and https://github.com/prasanthj/hyperloglog/blob/master/src/java/com/github/prasanthj/hll/Murmur3.java
 */
public enum MurmurHash3 {
    ;

    /**
     * A 128-bits hash.
     */
    public static class Hash128 {
        /** lower 64 bits part **/
        public long h1;
        /** higher 64 bits part **/
        public long h2;
    }

    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;
    private static final int R1 = 31;
    private static final int R2 = 27;
    private static final int M = 5;
    private static final int N1 = 0x52dce729;
    private static final int DEFAULT_SEED = 123;

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
    public static long hash64(byte[] data, int length) {
        return hash64(data, length, DEFAULT_SEED);
    }

    /**
     * Murmur3 64-bit variant. This is essentially MSB 8 bytes of Murmur3 128-bit variant.
     *
     * @param data   - input byte array
     * @param length - length of array
     * @param seed   - seed.
     * @return - hashcode
     */
    @SuppressWarnings("fall through")
    public static long hash64(byte[] data, int length, int seed) {
        long hash = seed;
        final int nblocks = length >> 3;

        // body
        for (int i = 0; i < nblocks; i++) {
            final int i8 = i << 3;
            long k = ((long) data[i8] & 0xff)
                     | (((long) data[i8 + 1] & 0xff) << 8)
                     | (((long) data[i8 + 2] & 0xff) << 16)
                     | (((long) data[i8 + 3] & 0xff) << 24)
                     | (((long) data[i8 + 4] & 0xff) << 32)
                     | (((long) data[i8 + 5] & 0xff) << 40)
                     | (((long) data[i8 + 6] & 0xff) << 48)
                     | (((long) data[i8 + 7] & 0xff) << 56);

            // mix functions
            k *= C1;
            k = Long.rotateLeft(k, R1);
            k *= C2;
            hash ^= k;
            hash = Long.rotateLeft(hash, R2) * M + N1;
        }

        // tail
        long k1 = 0;
        int tailStart = nblocks << 3;
        switch (length - tailStart) {
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

    /**
     * Compute the hash of the MurmurHash3_x64_128 hashing function.
     *
     * Note, this hashing function might be used to persist hashes, so if the way hashes are computed
     * changes for some reason, it needs to be addressed (like in BloomFilter and MurmurHashField).
     */
    @SuppressWarnings("fall through") // Intentionally uses fallthrough to implement a well known hashing algorithm
    public static Hash128 hash128(byte[] key, int offset, int length, long seed, Hash128 hash) {
        long h1 = seed;
        long h2 = seed;

        if (length >= 16) {

            final int len16 = length & 0xFFFFFFF0; // higher multiple of 16 that is lower than or equal to length
            final int end = offset + len16;
            for (int i = offset; i < end; i += 16) {
                long k1 = ByteUtils.readLongLE(key, i);

                k1 *= C1;
                k1 = Long.rotateLeft(k1, 31);
                k1 *= C2;
                h1 ^= k1;

                h1 = Long.rotateLeft(h1, 27);
                h1 += h2;
                h1 = h1 * 5 + 0x52dce729;

                long k2 = ByteUtils.readLongLE(key, i + 8);
                k2 *= C2;
                k2 = Long.rotateLeft(k2, 33);
                k2 *= C1;
                h2 ^= k2;

                h2 = Long.rotateLeft(h2, 31);
                h2 += h1;
                h2 = h2 * 5 + 0x38495ab5;
            }

            // Advance offset to the unprocessed tail of the data.
            offset = end;
        }

        long k1 = 0;
        long k2 = 0;

        switch (length & 15) {
            case 15:
                k2 ^= (key[offset + 14] & 0xFFL) << 48;
                // fall through
            case 14:
                k2 ^= (key[offset + 13] & 0xFFL) << 40;
                // fall through
            case 13:
                k2 ^= (key[offset + 12] & 0xFFL) << 32;
                // fall through
            case 12:
                k2 ^= (key[offset + 11] & 0xFFL) << 24;
                // fall through
            case 11:
                k2 ^= (key[offset + 10] & 0xFFL) << 16;
                // fall through
            case 10:
                k2 ^= (key[offset + 9] & 0xFFL) << 8;
                // fall through
            case 9:
                k2 ^= (key[offset + 8] & 0xFFL) << 0;
                k2 *= C2;
                k2 = Long.rotateLeft(k2, 33);
                k2 *= C1;
                h2 ^= k2;
                // fall through

            case 8:
                k1 ^= (key[offset + 7] & 0xFFL) << 56;
                // fall through
            case 7:
                k1 ^= (key[offset + 6] & 0xFFL) << 48;
                // fall through
            case 6:
                k1 ^= (key[offset + 5] & 0xFFL) << 40;
                // fall through
            case 5:
                k1 ^= (key[offset + 4] & 0xFFL) << 32;
                // fall through
            case 4:
                k1 ^= (key[offset + 3] & 0xFFL) << 24;
                // fall through
            case 3:
                k1 ^= (key[offset + 2] & 0xFFL) << 16;
                // fall through
            case 2:
                k1 ^= (key[offset + 1] & 0xFFL) << 8;
                // fall through
            case 1:
                k1 ^= (key[offset] & 0xFFL);
                k1 *= C1;
                k1 = Long.rotateLeft(k1, 31);
                k1 *= C2;
                h1 ^= k1;
                // fall through
            default:
                break;
        }

        h1 ^= length;
        h2 ^= length;

        h1 += h2;
        h2 += h1;

        h1 = fmix(h1);
        h2 = fmix(h2);

        h1 += h2;
        h2 += h1;

        hash.h1 = h1;
        hash.h2 = h2;
        return hash;
    }

}
