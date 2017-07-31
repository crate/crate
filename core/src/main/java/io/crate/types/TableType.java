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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * A type which represents a table retrieved by a subquery.
 * May be used in an IN or ANY operation, e.g. select 1 where 1 = ANY(Select 1)
 */
public class TableType extends DataType<Collection> implements CollectionType, Streamer<Object> {

    public static final int ID = 102;
    private DataType<?> valueType;

    public TableType(DataType<?> innerType) {
        this.valueType = innerType;
    }

    TableType() {}

    @Override
    public int id() {
        return ID;
    }

    @Override
    public String getName() {
        return valueType.getName() + "_table";
    }

    @Override
    public Streamer<?> streamer() {
        return this;
    }

    @Override
    public Collection value(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Collection) {
            return (Collection) value;
        }
        return Collections.singleton(valueType.value(value));
    }

    @Override
    public boolean isConvertableTo(DataType other) {
        return this.valueType.isConvertableTo(other) ||
               (other != null && other instanceof CollectionType &&
                ((CollectionType) other).innerType().isConvertableTo(this.valueType));
    }

    @Override
    public int compareValueTo(Collection val1, Collection val2) {
        if (val1 == val2 || (val1 != null && val1.equals(val2))) {
            return 0;
        }
        return -1;
    }

    @Override
    public int compareTo(Object o) {
        if (!(o instanceof TableType)) {
            return -1;
        }
        return Integer.compare(valueType.id(), ((TableType) o).valueType.id());
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        valueType = DataTypes.fromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        DataTypes.toStream(valueType, out);
    }

    @Override
    public Collection readValueFrom(StreamInput in) throws IOException {
        if (valueType.id() == SetType.ID || valueType.id() == ArrayType.ID) {
            return (Collection) valueType.streamer().readValueFrom(in);
        }
        int size = in.readVInt();
        switch (size) {
            case 0:
                return Collections.EMPTY_LIST;
            case 1:
                return Collections.singletonList(valueType.streamer().readValueFrom(in));
            default:
                List<Object> list = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    list.add(valueType.streamer().readValueFrom(in));
                }
                return list;
        }
    }

    @Override
    public void writeValueTo(StreamOutput out, Object value) throws IOException {
        if (value == null) {
            out.writeVInt(0);
            return;
        }
        if (valueType.id() == SetType.ID || valueType.id() == ArrayType.ID) {
            valueType.streamer().writeValueTo(out, value);
            return;
        }
        Collection collection = ((Collection) value);
        int size = collection.size();
        out.writeVInt(size);
        for (Object obj : collection) {
            valueType.streamer().writeValueTo(out, obj);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableType)) return false;
        if (!super.equals(o)) return false;

        TableType setType = (TableType) o;
        return valueType.equals(setType.valueType);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + valueType.hashCode();
        return result;
    }

    @Override
    public DataType<?> innerType() {
        if (valueType instanceof CollectionType){
            return ((CollectionType) valueType).innerType();
        }
        return valueType;
    }
}
