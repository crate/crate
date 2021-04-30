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

package io.crate.execution.engine.fetch;

/**
 * Utility class to pack/unpack a fetchId.
 * <p>
 *  A fetchId is a long which contains a readerId and a docId.
 *
 * readerIds are generated in the planner-phase. They're used to identify a shard/indexReader.
 * docIds are part of the lucene api, where they're used to identify documents within an IndexReader.
 * </p>
 *
 * <pre>
 *      4 bytes     |  4 bytes
 *         |             |
 *       readerId      docId
 * </pre>
 */
public final class FetchId {


    public static int decodeReaderId(long fetchId) {
        return (int) (fetchId >> 32);
    }

    public static int decodeDocId(long fetchId) {
        return (int) fetchId;
    }

    public static long encode(int readerId, int docId) {
        return ((long) readerId << 32) | (docId & 0xffffffffL);
    }
}
