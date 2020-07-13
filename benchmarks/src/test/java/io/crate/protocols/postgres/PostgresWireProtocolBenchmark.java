package io.crate.protocols.postgres;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Measurement(iterations = 4)
@Warmup(iterations = 3)
@State(Scope.Benchmark)
public class PostgresWireProtocolBenchmark {

    public ByteBuf buffer = Unpooled.wrappedBuffer("SELECT * FROM tbl WHERE x > 10 LIMIT 10".getBytes(StandardCharsets.UTF_8));

    static String oldReadCString(ByteBuf buffer) {
        byte[] bytes = new byte[buffer.bytesBefore((byte) 0) + 1];
        if (bytes.length == 0) {
            return null;
        }
        buffer.readBytes(bytes);
        return new String(bytes, 0, bytes.length - 1, StandardCharsets.UTF_8);
    }

    @Benchmark
    public String test_old_readCString() {
        return oldReadCString(buffer);
    }

    @Benchmark
    public String test_new_readCString() {
        return PostgresWireProtocol.readCString(buffer);
    }
}
