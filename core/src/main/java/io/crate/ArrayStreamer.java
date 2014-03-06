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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class ArrayStreamer<T> implements Streamer {

    private final Streamer<T> streamer;

    @SuppressWarnings("unchecked")
    public ArrayStreamer(Streamer streamer) {
        this.streamer = streamer;
    }

    @Override
    public Object readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        Object[] array = new Object[size];
        for (int i = 0; i < size; i++) {
            array[i] = streamer.readFrom(in);
        }
        return array;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void writeTo(StreamOutput out, Object v) throws IOException {
        Object[] array = (Object[]) v;
        out.writeVInt(array.length);
        for (Object t : array) {
            streamer.writeTo(out, t);
        }
    }
}
