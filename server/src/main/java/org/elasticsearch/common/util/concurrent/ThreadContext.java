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

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;


public final class ThreadContext {

    private ThreadContext() {
    }

    public static void bwcReadHeaders(StreamInput in) throws IOException {
        final int numRequest = in.readVInt();
        for (int i = 0; i < numRequest; i++) {
            in.readString();
            in.readString();
        }
        in.readMap(StreamInput::readString, input -> {
            final int size = input.readVInt();
            if (size == 0) {
                return Collections.emptySet();
            } else if (size == 1) {
                return Collections.singleton(input.readString());
            } else {
                // use a linked hash set to preserve order
                final LinkedHashSet<String> values = new LinkedHashSet<>(size);
                for (int i = 0; i < size; i++) {
                    final String value = input.readString();
                    final boolean added = values.add(value);
                    assert added : value;
                }
                return values;
            }
        });
    }

    public static void bwcWriteHeaders(StreamOutput out) throws IOException {
        out.writeVInt(0);
        out.writeVInt(0);
    }
}
