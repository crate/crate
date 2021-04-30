/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.jobs;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * <pre>
 * Benchmark                                                Mode  Cnt    Score      Error  Units
 * CheckingFreeMemoryBenchmark.benchmarkMemoryMXBeanMethod  avgt    4    0.332 ±    0.018  us/op
 * CheckingFreeMemoryBenchmark.benchmarkRuntimeMethod       avgt    4    0.075 ±    0.007  us/op
 * CheckingFreeMemoryBenchmark.benchmarkMemoryMXBeanMethod    ss    4  185.167 ± 2336.215  us/op
 * CheckingFreeMemoryBenchmark.benchmarkRuntimeMethod         ss    4    5.890 ±   46.856  us/op
 * </pre>
 */
@BenchmarkMode({Mode.AverageTime, Mode.SingleShotTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 1)
@Measurement(iterations = 4)
@State(Scope.Benchmark)
public class CheckFreeMemoryAlternativeMethodsBenchmark {

    private static abstract class HeapThresholdChecker implements Predicate<Void> {

        private final AtomicLong threshold;

        HeapThresholdChecker(int freeHeapPercent) {
            if (freeHeapPercent <= 0 || freeHeapPercent >= 100) {
                throw new IllegalArgumentException(String.format("value must be between 0..100: %d", freeHeapPercent));
            }
            threshold = new AtomicLong(Runtime.getRuntime().maxMemory() * freeHeapPercent / 100);
        }

        abstract long freeMemory();

        @Override
        public final boolean test(Void ignore) {
            return freeMemory() < threshold.get();
        }
    }

    private static class RuntimeMethod extends HeapThresholdChecker {

        private final Runtime rt;

        RuntimeMethod(int freeHeapPercent) {
            super(freeHeapPercent);
            rt = Runtime.getRuntime();
        }

        @Override
        long freeMemory() {
            return rt.freeMemory();
        }
    }

    private static class MemoryMXBeanMethod extends HeapThresholdChecker {

        private final MemoryMXBean memoryMXBean;

        MemoryMXBeanMethod(int freeHeapPercent) {
            super(freeHeapPercent);
            memoryMXBean = ManagementFactory.getMemoryMXBean();
        }

        @Override
        long freeMemory() {
            MemoryUsage memUse = memoryMXBean.getHeapMemoryUsage();
            return memUse.getMax() - memUse.getUsed();
        }
    }

    private RuntimeMethod runtimeMethod;
    private MemoryMXBeanMethod memoryMXBeanMethod;

    @Setup
    public void setup() {
        int freePercentageRequired = 30;
        runtimeMethod = new RuntimeMethod(freePercentageRequired);
        memoryMXBeanMethod = new MemoryMXBeanMethod(freePercentageRequired);
    }

    @Benchmark
    public void benchmarkRuntimeMethod() {
        runtimeMethod.test(null);
    }

    @Benchmark
    public void benchmarkMemoryMXBeanMethod() {
        memoryMXBeanMethod.test(null);
    }
}
