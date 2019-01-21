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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * This MessageDigests class provides convenience methods for obtaining
 * thread local {@link MessageDigest} instances for MD5, SHA-1, and
 * SHA-256 message digests.
 */
public final class MessageDigests {

    private static ThreadLocal<MessageDigest> createThreadLocalMessageDigest(String digest) {
        return ThreadLocal.withInitial(() -> {
            try {
                return MessageDigest.getInstance(digest);
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("unexpected exception creating MessageDigest instance for [" + digest + "]", e);
            }
        });
    }

    private static final ThreadLocal<MessageDigest> MD5_DIGEST = createThreadLocalMessageDigest("MD5");
    private static final ThreadLocal<MessageDigest> SHA_1_DIGEST = createThreadLocalMessageDigest("SHA-1");

    /**
     * Returns a {@link MessageDigest} instance for MD5 digests; note
     * that the instance returned is thread local and must not be
     * shared amongst threads.
     *
     * @return a thread local {@link MessageDigest} instance that
     * provides MD5 message digest functionality.
     */
    public static MessageDigest md5() {
        return get(MD5_DIGEST);
    }

    /**
     * Returns a {@link MessageDigest} instance for SHA-1 digests; note
     * that the instance returned is thread local and must not be
     * shared amongst threads.
     *
     * @return a thread local {@link MessageDigest} instance that
     * provides SHA-1 message digest functionality.
     */
    public static MessageDigest sha1() {
        return get(SHA_1_DIGEST);
    }

    private static MessageDigest get(ThreadLocal<MessageDigest> messageDigest) {
        MessageDigest instance = messageDigest.get();
        instance.reset();
        return instance;
    }
}
