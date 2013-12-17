package org.cratedb;

import com.google.common.collect.ImmutableSet;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Map;

public enum DataType {

    BYTE("byte", new Streamer<Byte>() {
        @Override
        public Byte readFrom(StreamInput in) throws IOException {
            return in.readByte();
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeByte((Byte) v);
        }
    }),
    SHORT("short", new Streamer<Short>() {
        @Override
        public Short readFrom(StreamInput in) throws IOException {
            return in.readShort();
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeShort((Short) v);
        }
    }),
    INTEGER("integer", new Streamer<Integer>() {
        @Override
        public Integer readFrom(StreamInput in) throws IOException {
            return in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeVInt((Integer) v);
        }
    }),
    LONG("long", new Streamer<Long>() {
        @Override
        public Long readFrom(StreamInput in) throws IOException {
            return in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeVLong((Long) v);
        }
    }),
    FLOAT("float", new Streamer<Float>() {
        @Override
        public Float readFrom(StreamInput in) throws IOException {
            return in.readFloat();
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeFloat((Float) v);
        }
    }),
    DOUBLE("double", new Streamer<Double>() {
        @Override
        public Double readFrom(StreamInput in) throws IOException {
            return in.readDouble();
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeDouble((Double) v);
        }
    }),
    BOOLEAN("boolean", new Streamer<Boolean>() {
        @Override
        public Boolean readFrom(StreamInput in) throws IOException {
            return in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeBoolean((Boolean) v);
        }
    }),
    STRING("string", Streamer.BYTES_REF),
    TIMESTAMP("timestamp", new Streamer<Long>() {
        @Override
        public Long readFrom(StreamInput in) throws IOException {
            return in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeLong((Long) v);
        }
    }),
    CRATY("craty", new Streamer<Object>() {
        @Override
        public Object readFrom(StreamInput in) throws IOException {
            return in.readGenericValue();
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            out.writeGenericValue(v);
        }
    }),
    IP("ip", Streamer.BYTES_REF);

    private final Streamer streamer;

    private String name;

    private DataType(String name, Streamer streamer) {
        this.name = name;
        this.streamer = streamer;
    }

    public String getName() {
        return this.name;
    }

    public Streamer streamer() {
        return streamer;
    }

    @Override
    public String toString() {
        return name;
    }

    public interface Streamer<T> {

        public T readFrom(StreamInput in) throws IOException;

        public void writeTo(StreamOutput out, Object v) throws IOException;

        public static final Streamer<BytesRef> BYTES_REF = new Streamer<BytesRef>() {
            @Override
            public BytesRef readFrom(StreamInput in) throws IOException {
                int length = in.readVInt();
                if (length == 0) {
                    return null;
                }
                byte[] bytes = new byte[length - 1];
                in.readBytes(bytes, 0, bytes.length);
                return new BytesRef(bytes);
            }

            @Override
            public void writeTo(StreamOutput out, Object v) throws IOException {
                if (v == null) {
                    out.writeVInt(0);
                } else {
                    BytesRef bytesRef = (BytesRef)v;
                    out.writeVInt(bytesRef.length + 1);
                    out.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                }
            }
        };
    }


    public static final ImmutableSet<DataType> NUMERIC_TYPES = ImmutableSet.of(
            BYTE,
            SHORT,
            INTEGER,
            LONG,
            FLOAT,
            DOUBLE
    );

    public static final ImmutableSet<DataType> ALL_TYPES = ImmutableSet.of(
            BYTE,
            SHORT,
            INTEGER,
            LONG,
            FLOAT,
            DOUBLE,
            BOOLEAN,
            STRING,
            TIMESTAMP,
            CRATY,
            IP
    );

    public static final ImmutableSet<DataType> PRIMITIVE_TYPES = ImmutableSet.of(
            BYTE,
            SHORT,
            INTEGER,
            LONG,
            FLOAT,
            DOUBLE,
            BOOLEAN,
            STRING,
            TIMESTAMP,
            IP
    );

    public static final ImmutableSet<DataType> INTEGER_TYPES = ImmutableSet.of(
            BYTE,
            SHORT,
            INTEGER,
            LONG
    );

    public static final ImmutableSet<DataType> DECIMAL_TYPES = ImmutableSet.of(
            FLOAT,
            DOUBLE
    );
}
