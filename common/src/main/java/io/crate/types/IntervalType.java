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

package io.crate.types;

import io.crate.Streamer;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.function.Function;

public class IntervalType extends DataType<Long> {

    public static final int ID = 17;

    private final Function<String, Long> parse = Long::parseLong;

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.IntervalType;
    }

    @Override
    public String getName() {
        return "interval";
    }

    @Override
    public Streamer<Long> streamer() {
        return null;
    }

    @Override
    public Long value(Object value) throws IllegalArgumentException, ClassCastException {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return parse.apply((String) value);
        }
        if (!(value instanceof Long)) {
            return ((Number) value).longValue();
        }
        return (Long) value;
    }

    public int compareValueTo(Long val1, Long val2) {
        return nullSafeCompareValueTo(val1, val2, Long::compare);
    }

}
