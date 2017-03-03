/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.types;

import io.crate.Streamer;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.*;

public class SetType extends DataType implements CollectionType, Streamer<Set> {

    public static final int ID = 101;

    private DataType innerType;

    SetType() {
    }

    public SetType(DataType innerType) {
        this.innerType = innerType;
    }

    @Override
    public DataType innerType() {
        return innerType;
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public String getName() {
        return innerType.getName() + "_set";
    }

    @Override
    public Streamer<?> streamer() {
        return this;
    }

    @Override
    public Set value(Object value) {
        if (value instanceof Set) {
            return (Set) value;
        }
        if (value == null) {
            return null;
        }
        if (value instanceof Collection) {
            //noinspection unchecked
            return new HashSet((Collection) value);
        }
        if (value.getClass().isArray()) {
            return new HashSet<>(Arrays.asList((Object[]) value));
        }
        throw new IllegalArgumentException(String.format(Locale.ENGLISH,
            "Cannot convert %s to %s", value, getName()));
    }

    @Override
    public boolean isConvertableTo(DataType other) {
        return other.id() == UndefinedType.ID ||
               ((other instanceof CollectionType)
                && this.innerType.isConvertableTo(((CollectionType) other).innerType()));
    }

    @Override
    public int compareValueTo(Object val1, Object val2) {
        if (val2 == null) {
            return 1;
        } else if (val1 == null) {
            return -1;
        } else if (this == val1) {
            return 0;
        }

        return Integer.compare(((Set) val1).size(), ((Set) val2).size());
    }

    @Override
    public int compareTo(Object o) {
        if (!(o instanceof SetType)) return -1;
        return Integer.compare(innerType.id(), ((SetType) o).innerType().id());
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        innerType = DataTypes.fromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        DataTypes.toStream(innerType, out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("inner_type", DataTypes.toXContent(innerType, builder, params));
        return builder;
    }

    @Override
    public void fromXContent(XContentParser parser) throws IOException {
        if (parser.nextToken() != XContentParser.Token.FIELD_NAME || !"inner_type".equals(parser.currentName())) {
            throw new IllegalStateException("Can't parse DataType form XContent");
        }
        innerType = DataTypes.fromXContent(parser);
    }

    @Override
    public Set readValueFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        Set<Object> s = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            s.add(innerType.streamer().readValueFrom(in));
        }
        if (in.readBoolean()) {
            s.add(null);
        }
        return s;
    }

    @Override
    public void writeValueTo(StreamOutput out, Object v) throws IOException {
        assert v instanceof Set : "v must be instance of Set";
        Set s = (Set) v;
        boolean containsNull = s.contains(null);
        out.writeVInt(containsNull ? s.size() - 1 : s.size());
        for (Object e : s) {
            if (e == null) {
                continue;
            }
            innerType.streamer().writeValueTo(out, e);
        }
        out.writeBoolean(containsNull);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SetType)) return false;
        if (!super.equals(o)) return false;

        SetType setType = (SetType) o;

        if (!innerType.equals(setType.innerType)) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + innerType.hashCode();
        return result;
    }
}
