/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.testing;

import io.crate.data.breaker.RamAccounting;

public class PlainRamAccounting implements RamAccounting {

    long total;
    long breakingThreshold;
    boolean closed;

    public PlainRamAccounting() {
        this.breakingThreshold = -1;
    }

    public PlainRamAccounting(long breakingThreshold) {
        this.breakingThreshold = breakingThreshold;
    }

    @Override
    public void addBytes(long bytes) {
        total += bytes;
        if (breakingThreshold != -1 && total > breakingThreshold) {
            // break when more than two block sizes have been added to the Sketch
            throw new RuntimeException("Circuit break! " + total);
        }

    }

    @Override
    public long totalBytes() {
        return total;
    }

    @Override
    public void release() {
        total = 0;
    }

    @Override
    public void close() {
        closed = true;
    }

    public boolean closed() {
        return closed;
    }
}
