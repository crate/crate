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

package io.crate.data;

import io.crate.testing.RowGenerator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import static io.crate.data.SentinelRow.SENTINEL;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 2)
@Fork(value = 2)
@Measurement(iterations = 5)
public class BatchIteratorConsumeBenchmark {

    static final int numItems = 10_000_000;

    private Iterable<Row> rows = () -> StreamSupport.stream(RowGenerator.range(0, numItems).spliterator(), false).iterator();

    private Iterable<Row> columnRows = MyIterator::new;

    private SingleIntArrayBlock arrayBlock = new SingleIntArrayBlock();

    @Benchmark
    public void measure_consume_with_shared_row_iterator(Blackhole blackhole) {
        BatchIterator<Row> it = new InMemoryBatchIterator<>(rows, SENTINEL, false);
        while (it.moveNext()) {
            blackhole.consume(it.currentElement().get(0));
        }
    }

    @Benchmark
    public void measure_consume_with_column_oriented_array_backend(Blackhole blackhole) {
        BatchIterator<Row> it = new InMemoryBatchIterator<>(columnRows, SENTINEL, false);
        while (it.moveNext()) {
            blackhole.consume(it.currentElement().get(0));
        }
    }

    @Benchmark
    public void measure_consume_from_block_to_row_conversion(Blackhole blackhole) {
        var it = new FlatMapBatchIterator<Block, Row>(
            new InMemoryBatchIterator<>(List.of(arrayBlock), null, false),
            block -> new Iterator<>() {

                int idx = -1;
                final Row row = new Row() {
                    @Override
                    public int numColumns() {
                        return 1;
                    }

                    @Override
                    public Object get(int index) {
                        return block.vectors().get(index).getInt(idx);
                    }
                };

                @Override
                public boolean hasNext() {
                    return idx + 1 < numItems;
                }

                @Override
                public Row next() {
                    idx++;
                    return row;
                }
            }
        );
        while (it.moveNext()) {
            blackhole.consume(it.currentElement().get(0));
        }
    }

    @Benchmark
    public void measure_consume_with_multiplication_on_shared_row(Blackhole blackhole) {
        BatchIterator<Row> it = BatchIterators.map(
            new InMemoryBatchIterator<>(rows, SENTINEL, false),
            new Function<>() {

                Row innerRow = Row.EMPTY;
                Row mappingRow = new Row() {

                    @Override
                    public int numColumns() {
                        return innerRow.numColumns();
                    }

                    @Override
                    public Object get(int index) {
                        return ((Integer) innerRow.get(index)) * 3;
                    }
                };

                @Override
                public Row apply(Row row) {
                    innerRow = row;
                    return mappingRow;
                }
            }
        );
        while (it.moveNext()) {
            blackhole.consume(it.currentElement().get(0));
        }
    }

    @Benchmark
    public void measure_consume_with_multiplication_on_block(Blackhole blackhole) {
        var blockInMemoryBatchIterator = BatchIterators.map(
            new InMemoryBatchIterator<Block>(List.of(arrayBlock), null, false),
            block -> {
                ByteBuf buf = block.vectors().get(0);
                for (int i = 0; i < numItems; i++) {
                    buf.setInt(i, buf.getInt(i) * 3);
                }
                return block;
            }
        );
        var it = new FlatMapBatchIterator<Block, Row>(
            blockInMemoryBatchIterator,
            block -> new Iterator<>() {

                int idx = -1;
                final Row row = new Row() {
                    @Override
                    public int numColumns() {
                        return 1;
                    }

                    @Override
                    public Object get(int index) {
                        return block.vectors().get(index).getInt(idx);
                    }
                };

                @Override
                public boolean hasNext() {
                    return idx + 1 < numItems;
                }

                @Override
                public Row next() {
                    idx++;
                    return row;
                }
            }
        );
        while (it.moveNext()) {
            blackhole.consume(it.currentElement().get(0));
        }
    }

    static class SingleIntArrayBlock implements Block {

        private final List<ByteBuf> vectors;

        public SingleIntArrayBlock() {
            ByteBuf buffer = Unpooled.buffer(Integer.SIZE * numItems);
            for (int i = 0; i < numItems; i++) {
                buffer.setInt(i, i);
            }
            vectors = List.of(buffer);
        }

        @Override
        public List<ByteBuf> vectors() {
            return vectors;
        }
    }

    interface Block {

        List<ByteBuf> vectors();
    }

    private static class MyIterator implements Iterator<Row> {

        final int[] values = new int[numItems];
        int idx = -1;

        private final Row row = new Row() {
            @Override
            public int numColumns() {
                return 1;
            }

            @Override
            public Object get(int index) {
                return values[idx];
            }
        };

        @Override
        public boolean hasNext() {
            return idx + 1 < values.length;
        }

        @Override
        public Row next() {
            idx++;
            return row;
        }
    }
}
