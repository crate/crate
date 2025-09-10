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

package org.elasticsearch.cluster.health;


import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

public enum Health implements Writeable {
    GREEN,
    YELLOW,
    RED();

    public short severity() {
        return (short) (ordinal() + 1);
    }

    public byte value() {
        return (byte) ordinal();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(value());
    }

    /**
     * Read from a stream.
     *
     * @throws IllegalArgumentException if the value is unrecognized
     */
    public static Health readFrom(StreamInput in) throws IOException {
        return fromValue(in.readByte());
    }

    public static Health fromValue(byte value) throws IOException {
        return switch (value) {
            case 0 -> GREEN;
            case 1 -> YELLOW;
            case 2 -> RED;
            default -> throw new IllegalArgumentException("No cluster health status for value [" + value + "]");
        };
    }

    public static Health fromString(String status) {
        if (status.equalsIgnoreCase("green")) {
            return GREEN;
        } else if (status.equalsIgnoreCase("yellow")) {
            return YELLOW;
        } else if (status.equalsIgnoreCase("red")) {
            return RED;
        } else {
            throw new IllegalArgumentException("unknown cluster health status [" + status + "]");
        }
    }
}
