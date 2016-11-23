/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.reference.doc.lucene;

/*
 * Utilities to pack and extract fields of fetchId
 *
 * Pack relationId, readerId and docId into one long.
 *  1 byte       3 bytes   |  4 bytes
 *     ^           ^             ^
 *     |           |             |
 * relationId   readerId       docId
 */
public final class FetchIds {

    public static long packFetchId(byte relationId, long readerId, int docId) {
        return (((relationId << 24) | (readerId & 0x00ffffff)) << 32) | (docId & 0xffffffff);
    }

    public static byte extractRelationId(long fetchId) {
        return (byte)((fetchId >> 56) & 0x000000ff);
    }

    public static int extractReaderId(long fetchId) {
        return (int) ((fetchId >> 32) & 0x00ffffff);
    }

    public static int extractDocId(long fetchId) {
        return (int) fetchId;
    }
}
