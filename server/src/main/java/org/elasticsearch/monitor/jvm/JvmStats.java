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

package org.elasticsearch.monitor.jvm;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import io.crate.common.unit.TimeValue;

import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ClassLoadingMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class JvmStats implements Writeable {

    private static final RuntimeMXBean RUNTIME_MXBEAN;
    private static final MemoryMXBean MEMORY_MXBEAN;
    private static final ThreadMXBean THREAD_MXBEAN;
    private static final ClassLoadingMXBean CLASS_LOADING_MXBEAN;

    static {
        RUNTIME_MXBEAN = ManagementFactory.getRuntimeMXBean();
        MEMORY_MXBEAN = ManagementFactory.getMemoryMXBean();
        THREAD_MXBEAN = ManagementFactory.getThreadMXBean();
        CLASS_LOADING_MXBEAN = ManagementFactory.getClassLoadingMXBean();
    }

    public static JvmStats jvmStats() {
        MemoryUsage memUsage = MEMORY_MXBEAN.getHeapMemoryUsage();
        long heapUsed = memUsage.getUsed() < 0 ? 0 : memUsage.getUsed();
        long heapCommitted = memUsage.getCommitted() < 0 ? 0 : memUsage.getCommitted();
        long heapMax = memUsage.getMax() < 0 ? 0 : memUsage.getMax();
        memUsage = MEMORY_MXBEAN.getNonHeapMemoryUsage();
        long nonHeapUsed = memUsage.getUsed() < 0 ? 0 : memUsage.getUsed();
        long nonHeapCommitted = memUsage.getCommitted() < 0 ? 0 : memUsage.getCommitted();
        List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
        List<MemoryPool> pools = new ArrayList<>();
        for (MemoryPoolMXBean memoryPoolMXBean : memoryPoolMXBeans) {
            try {
                MemoryUsage usage = memoryPoolMXBean.getUsage();
                MemoryUsage peakUsage = memoryPoolMXBean.getPeakUsage();
                String name = GcNames.getByMemoryPoolName(memoryPoolMXBean.getName(), null);
                if (name == null) { // if we can't resolve it, its not interesting.... (Per Gen, Code Cache)
                    continue;
                }
                pools.add(new MemoryPool(name,
                        usage.getUsed() < 0 ? 0 : usage.getUsed(),
                        usage.getMax() < 0 ? 0 : usage.getMax(),
                        peakUsage.getUsed() < 0 ? 0 : peakUsage.getUsed(),
                        peakUsage.getMax() < 0 ? 0 : peakUsage.getMax()
                ));
            } catch (final Exception ignored) {

            }
        }
        Mem mem = new Mem(heapCommitted, heapUsed, heapMax, nonHeapCommitted, nonHeapUsed, Collections.unmodifiableList(pools));
        Threads threads = new Threads(THREAD_MXBEAN.getThreadCount(), THREAD_MXBEAN.getPeakThreadCount());

        List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        GarbageCollector[] collectors = new GarbageCollector[gcMxBeans.size()];
        for (int i = 0; i < collectors.length; i++) {
            GarbageCollectorMXBean gcMxBean = gcMxBeans.get(i);
            collectors[i] = new GarbageCollector(GcNames.getByGcName(gcMxBean.getName(), gcMxBean.getName()),
                    gcMxBean.getCollectionCount(), gcMxBean.getCollectionTime());
        }
        GarbageCollectors garbageCollectors = new GarbageCollectors(collectors);
        List<BufferPool> bufferPoolsList = Collections.emptyList();
        try {
            List<BufferPoolMXBean> bufferPools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
            bufferPoolsList = new ArrayList<>(bufferPools.size());
            for (BufferPoolMXBean bufferPool : bufferPools) {
                bufferPoolsList.add(new BufferPool(bufferPool.getName(), bufferPool.getCount(),
                        bufferPool.getTotalCapacity(), bufferPool.getMemoryUsed()));
            }
        } catch (Exception e) {
            // buffer pools are not available
        }

        Classes classes = new Classes(CLASS_LOADING_MXBEAN.getLoadedClassCount(), CLASS_LOADING_MXBEAN.getTotalLoadedClassCount(),
                CLASS_LOADING_MXBEAN.getUnloadedClassCount());

        return new JvmStats(System.currentTimeMillis(), RUNTIME_MXBEAN.getUptime(), mem, threads,
                garbageCollectors, bufferPoolsList, classes);
    }

    private final long timestamp;
    private final long uptime;
    private final Mem mem;
    private final Threads threads;
    private final GarbageCollectors gc;
    private final List<BufferPool> bufferPools;
    private final Classes classes;

    public JvmStats(long timestamp, long uptime, Mem mem, Threads threads, GarbageCollectors gc,
                    List<BufferPool> bufferPools, Classes classes) {
        this.timestamp = timestamp;
        this.uptime = uptime;
        this.mem = mem;
        this.threads = threads;
        this.gc = gc;
        this.bufferPools = bufferPools;
        this.classes = classes;
    }

    public JvmStats(StreamInput in) throws IOException {
        timestamp = in.readVLong();
        uptime = in.readVLong();
        mem = new Mem(in);
        threads = new Threads(in);
        gc = new GarbageCollectors(in);
        bufferPools = in.readList(BufferPool::new);
        classes = new Classes(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(timestamp);
        out.writeVLong(uptime);
        mem.writeTo(out);
        threads.writeTo(out);
        gc.writeTo(out);
        out.writeList(bufferPools);
        classes.writeTo(out);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public TimeValue getUptime() {
        return new TimeValue(uptime);
    }

    public Mem getMem() {
        return this.mem;
    }

    public Threads getThreads() {
        return threads;
    }

    public GarbageCollectors getGc() {
        return gc;
    }

    public List<BufferPool> getBufferPools() {
        return bufferPools;
    }

    public Classes getClasses() {
        return classes;
    }

    public static class GarbageCollectors implements Writeable, Iterable<GarbageCollector> {

        private final GarbageCollector[] collectors;

        public GarbageCollectors(GarbageCollector[] collectors) {
            this.collectors = collectors;
        }

        public GarbageCollectors(StreamInput in) throws IOException {
            collectors = in.readArray(GarbageCollector::new, GarbageCollector[]::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeArray(collectors);
        }

        public GarbageCollector[] getCollectors() {
            return this.collectors;
        }

        @Override
        public Iterator<GarbageCollector> iterator() {
            return Arrays.stream(collectors).iterator();
        }
    }

    public static class GarbageCollector implements Writeable {

        private final String name;
        private final long collectionCount;
        private final long collectionTime;

        public GarbageCollector(String name, long collectionCount, long collectionTime) {
            this.name = name;
            this.collectionCount = collectionCount;
            this.collectionTime = collectionTime;
        }

        public GarbageCollector(StreamInput in) throws IOException {
            name = in.readString();
            collectionCount = in.readVLong();
            collectionTime = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVLong(collectionCount);
            out.writeVLong(collectionTime);
        }

        public String getName() {
            return this.name;
        }

        public long getCollectionCount() {
            return this.collectionCount;
        }

        public TimeValue getCollectionTime() {
            return new TimeValue(collectionTime, TimeUnit.MILLISECONDS);
        }
    }

    public static class Threads implements Writeable {

        private final int count;
        private final int peakCount;

        public Threads(int count, int peakCount) {
            this.count = count;
            this.peakCount = peakCount;
        }

        public Threads(StreamInput in) throws IOException {
            count = in.readVInt();
            peakCount = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(count);
            out.writeVInt(peakCount);
        }

        public int getCount() {
            return count;
        }

        public int getPeakCount() {
            return peakCount;
        }
    }

    public static class MemoryPool implements Writeable {

        private final String name;
        private final long used;
        private final long max;
        private final long peakUsed;
        private final long peakMax;

        public MemoryPool(String name, long used, long max, long peakUsed, long peakMax) {
            this.name = name;
            this.used = used;
            this.max = max;
            this.peakUsed = peakUsed;
            this.peakMax = peakMax;
        }

        public MemoryPool(StreamInput in) throws IOException {
            name = in.readString();
            used = in.readVLong();
            max = in.readVLong();
            peakUsed = in.readVLong();
            peakMax = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVLong(used);
            out.writeVLong(max);
            out.writeVLong(peakUsed);
            out.writeVLong(peakMax);
        }

        public String getName() {
            return this.name;
        }

        public ByteSizeValue getUsed() {
            return new ByteSizeValue(used);
        }

        public ByteSizeValue getMax() {
            return new ByteSizeValue(max);
        }

        public ByteSizeValue getPeakUsed() {
            return new ByteSizeValue(peakUsed);
        }

        public ByteSizeValue getPeakMax() {
            return new ByteSizeValue(peakMax);
        }
    }

    public static class Mem implements Writeable, Iterable<MemoryPool> {

        private final long heapCommitted;
        private final long heapUsed;
        private final long heapMax;
        private final long nonHeapCommitted;
        private final long nonHeapUsed;
        private final List<MemoryPool> pools;

        public Mem(long heapCommitted, long heapUsed, long heapMax, long nonHeapCommitted, long nonHeapUsed, List<MemoryPool> pools) {
            this.heapCommitted = heapCommitted;
            this.heapUsed = heapUsed;
            this.heapMax = heapMax;
            this.nonHeapCommitted = nonHeapCommitted;
            this.nonHeapUsed = nonHeapUsed;
            this.pools = pools;
        }

        public Mem(StreamInput in) throws IOException {
            heapCommitted = in.readVLong();
            heapUsed = in.readVLong();
            nonHeapCommitted = in.readVLong();
            nonHeapUsed = in.readVLong();
            heapMax = in.readVLong();
            pools = in.readList(MemoryPool::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(heapCommitted);
            out.writeVLong(heapUsed);
            out.writeVLong(nonHeapCommitted);
            out.writeVLong(nonHeapUsed);
            out.writeVLong(heapMax);
            out.writeList(pools);
        }

        @Override
        public Iterator<MemoryPool> iterator() {
            return pools.iterator();
        }

        public ByteSizeValue getHeapCommitted() {
            return new ByteSizeValue(heapCommitted);
        }

        public ByteSizeValue getHeapUsed() {
            return new ByteSizeValue(heapUsed);
        }

        /**
         * returns the maximum heap size. 0 bytes signals unknown.
         */
        public ByteSizeValue getHeapMax() {
            return new ByteSizeValue(heapMax);
        }

        /**
         * returns the heap usage in percent. -1 signals unknown.
         */
        public short getHeapUsedPercent() {
            if (heapMax == 0) {
                return -1;
            }
            return (short) (heapUsed * 100 / heapMax);
        }

        public ByteSizeValue getNonHeapCommitted() {
            return new ByteSizeValue(nonHeapCommitted);
        }

        public ByteSizeValue getNonHeapUsed() {
            return new ByteSizeValue(nonHeapUsed);
        }
    }

    public static class BufferPool implements Writeable {

        private final String name;
        private final long count;
        private final long totalCapacity;
        private final long used;

        public BufferPool(String name, long count, long totalCapacity, long used) {
            this.name = name;
            this.count = count;
            this.totalCapacity = totalCapacity;
            this.used = used;
        }

        public BufferPool(StreamInput in) throws IOException {
            name = in.readString();
            count = in.readLong();
            totalCapacity = in.readLong();
            used = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeLong(count);
            out.writeLong(totalCapacity);
            out.writeLong(used);
        }

        public String getName() {
            return this.name;
        }

        public long getCount() {
            return this.count;
        }

        public ByteSizeValue getTotalCapacity() {
            return new ByteSizeValue(totalCapacity);
        }

        public ByteSizeValue getUsed() {
            return new ByteSizeValue(used);
        }
    }

    public static class Classes implements Writeable {

        private final long loadedClassCount;
        private final long totalLoadedClassCount;
        private final long unloadedClassCount;

        public Classes(long loadedClassCount, long totalLoadedClassCount, long unloadedClassCount) {
            this.loadedClassCount = loadedClassCount;
            this.totalLoadedClassCount = totalLoadedClassCount;
            this.unloadedClassCount = unloadedClassCount;
        }

        public Classes(StreamInput in) throws IOException {
            loadedClassCount = in.readLong();
            totalLoadedClassCount = in.readLong();
            unloadedClassCount = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(loadedClassCount);
            out.writeLong(totalLoadedClassCount);
            out.writeLong(unloadedClassCount);
        }

        public long getLoadedClassCount() {
            return loadedClassCount;
        }

        public long getTotalLoadedClassCount() {
            return totalLoadedClassCount;
        }

        public long getUnloadedClassCount() {
            return unloadedClassCount;
        }
    }
}
