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

import com.google.common.base.Preconditions;
import io.crate.Streamer;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * A type which contains a collection of elements of another type.
 */
public abstract class CollectionType extends DataType {

    protected DataType<?> innerType;
    protected Streamer streamer;

    /**
     * Construct a new Collection type
     * @param innerType The type of the elements inside the collection
     */
    public CollectionType(DataType<?> innerType) {
        this.innerType = Preconditions.checkNotNull(innerType,
            "Inner type must not be null.");
    }

    /**
     * Constructor used for the {@link org.elasticsearch.common.io.stream.Streamable}
     * interface which initializes the fields after object creation.
     */
    public CollectionType() {}

    /**
     * Defaults to the {@link ArrayStreamer} but subclasses may override this method.
     */
    @Override
    public Streamer<?> streamer() {
        if (streamer == null) {
            streamer = new ArrayStreamer(innerType);
        }
        return streamer;
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
    public String getName() {
        return innerType.getName();
    }

    public final DataType<?> innerType() {
        return innerType;
    }

    /**
     * Returns a new instance of this CollectionType with a given inner type.
     * @param innerType The inner type of the new CollectionType.
     */
    public abstract CollectionType newInstance(DataType innerType);

    @Override
    public boolean isNumeric() {
        return innerType.isNumeric();
    }

    @Override
    public int compareTo(Object o) {
        if (!(o instanceof CollectionType)) return -1;
        return Integer.compare(innerType.id(), ((CollectionType) o).innerType().id());
    }

    @Override
    public int compareValueTo(Object val1, Object val2) {
        if (val2 == null) {
            return 1;
        } else if (val1 == null) {
            return -1;
        }
        if (val1 instanceof Collection) {
            return val1.equals(val2) ? 0 : 1;
        }
        return Arrays.deepEquals((Object[]) val1, (Object[]) val2) ? 0 : 1;
    }

    @Override
    public boolean isConvertableTo(DataType other) {
        return other.id() == UndefinedType.ID ||
               ((other instanceof CollectionType)
                && this.innerType.isConvertableTo(((CollectionType) other).innerType()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CollectionType)) return false;
        if (!super.equals(o)) return false;

        CollectionType collectionType = (CollectionType) o;
        return innerType.equals(collectionType.innerType);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + innerType.hashCode();
        return result;
    }

    public static DataType unnest(DataType dataType) {
        while (DataTypes.isCollectionType(dataType)) {
            dataType = ((CollectionType) dataType).innerType();
        }
        return dataType;
    }

    static class ArrayStreamer implements Streamer {

        private DataType innerType;

        ArrayStreamer(DataType innerType) {
            this.innerType = innerType;
        }

        @Override
        public Object[] readValueFrom(StreamInput in) throws IOException {
            int size = in.readVInt();
            // size of 0 is treated as null value so real size must be decreased by 1
            if (size == 0) {
                return null;
            }
            size--;
            Object[] array = new Object[size];
            for (int i = 0; i < size; i++) {
                array[i] = innerType.streamer().readValueFrom(in);
            }
            return array;
        }

        @Override
        public void writeValueTo(StreamOutput out, Object values) throws IOException {
            // write null as size 0, so increase real size by 1
            if (values == null) {
                out.writeVInt(0);
                return;
            }
            Object[] array = (Object[]) values;
            out.writeVInt(array.length + 1);
            for (Object value : array) {
                innerType.streamer().writeValueTo(out, value);
            }
        }
    }
}
