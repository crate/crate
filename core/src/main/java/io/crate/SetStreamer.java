/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class SetStreamer<T> implements Streamer {

    private final Streamer<T> streamer;

    public SetStreamer(Streamer streamer) {
        this.streamer = streamer;
    }


    @Override
    public Set<T> readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        Set<T> s = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            s.add(streamer.readFrom(in));
        }
        if (in.readBoolean()) {
            s.add(null);
        }
        return s;
    }

    @Override
    public void writeTo(StreamOutput out, Object v) throws IOException {
        Set<T> s = (Set<T>) v;
        boolean containsNull = s.contains(null);
        out.writeVInt(containsNull ? s.size() - 1 : s.size());
        for (T e : s) {
            if (e == null) {
                continue;
            }
            streamer.writeTo(out, e);
        }
        out.writeBoolean(containsNull);
    }


    public static final Streamer<Set<BytesRef>> BYTES_REF_SET = new SetStreamer<BytesRef>(BYTES_REF);

}
