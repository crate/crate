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

package io.crate.collections;

import org.apache.lucene.util.ArrayUtil;

public class ByteList {

    private static int DEFAULT_EXPECTED_ITEMS = 4;

    public byte[] buffer;
    public int numItems;

    public ByteList() {
        this.buffer = new byte[DEFAULT_EXPECTED_ITEMS];
    }

    public void add(byte b) {
        ensureSpace(1);
        buffer[numItems++] = b;
    }

    public void add(byte ... bytes) {
        ensureSpace(bytes.length);
        System.arraycopy(bytes, 0, buffer, numItems, bytes.length);
        numItems += bytes.length;
    }

    private void ensureSpace(int additions) {
        if (numItems + additions > buffer.length) {
            this.buffer = ArrayUtil.grow(buffer, numItems + additions);
        }
    }

    /** Return an array that matches exactly the number of elements */
    public byte[] toArray() {
        return ArrayUtil.copyOfSubArray(buffer, 0, numItems);
    }
}
