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

package org.elasticsearch.indices.recovery;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.jspecify.annotations.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import io.crate.common.unit.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Keeps track of state related to shard recovery.
 */
public class RecoveryState implements Writeable {

    public enum Stage {
        INIT((byte) 0),

        /**
         * recovery of lucene files, either reusing local ones are copying new ones
         */
        INDEX((byte) 1),

        /**
         * potentially running check index
         */
        VERIFY_INDEX((byte) 2),

        /**
         * starting up the engine, replaying the translog
         */
        TRANSLOG((byte) 3),

        /**
         * performing final task after all translog ops have been done
         */
        FINALIZE((byte) 4),

        DONE((byte) 5);

        private static final Stage[] STAGES = new Stage[Stage.values().length];

        static {
            for (Stage stage : Stage.values()) {
                assert stage.id() < STAGES.length && stage.id() >= 0;
                STAGES[stage.id] = stage;
            }
        }

        private final byte id;

        Stage(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static Stage fromId(byte id) {
            if (id < 0 || id >= STAGES.length) {
                throw new IllegalArgumentException("No mapping for id [" + id + "]");
            }
            return STAGES[id];
        }
    }

    private Stage stage;

    private final Index index;
    private final Translog translog;
    private final VerifyIndex verifyIndex;
    private final Timer timer;
    private final boolean primary;

    private RecoverySource recoverySource;
    private ShardId shardId;
    @Nullable
    private DiscoveryNode sourceNode;
    private DiscoveryNode targetNode;

    public RecoveryState(ShardRouting shardRouting, DiscoveryNode targetNode, @Nullable DiscoveryNode sourceNode) {
        assert shardRouting.initializing() : "only allow initializing shard routing to be recovered: " + shardRouting;
        RecoverySource recoverySource = shardRouting.recoverySource();
        assert (recoverySource.getType() == RecoverySource.Type.PEER) == (sourceNode != null) :
            "peer recovery requires source node, recovery type: " + recoverySource.getType() + " source node: " + sourceNode;
        this.shardId = shardRouting.shardId();
        this.primary = shardRouting.primary();
        this.recoverySource = recoverySource;
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        this.stage = Stage.INIT;
        this.timer = new Timer();
        this.timer.start();
        this.index = new Index();
        this.translog = new Translog();
        this.verifyIndex = new VerifyIndex();
    }

    public ShardId getShardId() {
        return shardId;
    }

    public synchronized Stage getStage() {
        return this.stage;
    }


    private void validateAndSetStage(Stage expected, Stage next) {
        if (stage != expected) {
            assert false : "can't move recovery to stage [" + next + "]. current stage: [" + stage + "] (expected [" + expected + "])";
            throw new IllegalStateException("can't move recovery to stage [" + next + "]. current stage: ["
                    + stage + "] (expected [" + expected + "])");
        }
        stage = next;
    }

    public synchronized void validateCurrentStage(Stage expected) {
        if (stage != expected) {
            assert false : "expected stage [" + expected + "]; but current stage is [" + stage + "]";
            throw new IllegalStateException("expected stage [" + expected + "] but current stage is [" + stage + "]");
        }
    }

    // synchronized is strictly speaking not needed (this is called by a single thread), but just to be safe
    public synchronized RecoveryState setStage(Stage stage) {
        switch (stage) {
            case INIT:
                // reinitializing stop remove all state except for start time
                this.stage = Stage.INIT;
                getIndex().reset();
                getVerifyIndex().reset();
                getTranslog().reset();
                break;
            case INDEX:
                validateAndSetStage(Stage.INIT, stage);
                getIndex().start();
                break;
            case VERIFY_INDEX:
                validateAndSetStage(Stage.INDEX, stage);
                getIndex().stop();
                getVerifyIndex().start();
                break;
            case TRANSLOG:
                validateAndSetStage(Stage.VERIFY_INDEX, stage);
                getVerifyIndex().stop();
                getTranslog().start();
                break;
            case FINALIZE:
                assert getIndex().bytesStillToRecover() >= 0 : "moving to stage FINALIZE without completing file details";
                validateAndSetStage(Stage.TRANSLOG, stage);
                getTranslog().stop();
                break;
            case DONE:
                validateAndSetStage(Stage.FINALIZE, stage);
                getTimer().stop();
                break;
            default:
                throw new IllegalArgumentException("unknown RecoveryState.Stage [" + stage + "]");
        }
        return this;
    }

    public Index getIndex() {
        return index;
    }

    public VerifyIndex getVerifyIndex() {
        return this.verifyIndex;
    }

    public Translog getTranslog() {
        return translog;
    }

    public Timer getTimer() {
        return timer;
    }

    public RecoverySource getRecoverySource() {
        return recoverySource;
    }

    /**
     * Returns recovery source node (only non-null if peer recovery)
     */
    @Nullable
    public DiscoveryNode getSourceNode() {
        return sourceNode;
    }

    public DiscoveryNode getTargetNode() {
        return targetNode;
    }

    public boolean getPrimary() {
        return primary;
    }

    public RecoveryState(StreamInput in) throws IOException {
        timer = new Timer(in);
        stage = Stage.fromId(in.readByte());
        shardId = new ShardId(in);
        recoverySource = RecoverySource.readFrom(in);
        targetNode = new DiscoveryNode(in);
        sourceNode = in.readOptionalWriteable(DiscoveryNode::new);
        index = new Index(in);
        translog = new Translog(in);
        verifyIndex = new VerifyIndex(in);
        primary = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        timer.writeTo(out);
        out.writeByte(stage.id());
        shardId.writeTo(out);
        recoverySource.writeTo(out);
        targetNode.writeTo(out);
        out.writeOptionalWriteable(sourceNode);
        index.writeTo(out);
        translog.writeTo(out);
        verifyIndex.writeTo(out);
        out.writeBoolean(primary);
    }

    public static class Timer implements Writeable {

        protected long startTime;
        protected long startNanoTime;
        protected long time;
        protected long stopTime;

        public Timer() {
            startTime = 0;
            startNanoTime = 0;
            time = -1;
            stopTime = 0;
        }

        public synchronized void start() {
            assert startTime == 0 : "already started";
            startTime = System.currentTimeMillis();
            startNanoTime = System.nanoTime();
        }

        /** Returns start time in millis */
        public synchronized long startTime() {
            return startTime;
        }

        /** Returns elapsed time in millis, or 0 if timer was not started */
        public synchronized long time() {
            if (startNanoTime == 0) {
                return 0;
            }
            if (time >= 0) {
                return time;
            }
            return Math.max(0, TimeValue.nsecToMSec(System.nanoTime() - startNanoTime));
        }

        public synchronized void stop() {
            assert stopTime == 0 : "already stopped";
            stopTime = Math.max(System.currentTimeMillis(), startTime);
            time = TimeValue.nsecToMSec(System.nanoTime() - startNanoTime);
            assert time >= 0;
        }

        public synchronized void reset() {
            startTime = 0;
            startNanoTime = 0;
            time = -1;
            stopTime = 0;
        }

        // for tests
        public long getStartNanoTime() {
            return startNanoTime;
        }

        public Timer(StreamInput in) throws IOException {
            startTime = in.readVLong();
            startNanoTime = in.readVLong();
            stopTime = in.readVLong();
            time = in.readVLong();
        }

        @Override
        public synchronized void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(startTime);
            out.writeVLong(startNanoTime);
            out.writeVLong(stopTime);
            // write a snapshot of current time, which is not per se the time field
            out.writeVLong(time());
        }
    }

    public static class VerifyIndex extends Timer implements Writeable {

        private volatile long checkIndexTime;

        public VerifyIndex() {
            super();
            checkIndexTime = 0;
        }

        @Override
        @SuppressWarnings("sync-override")
        public void reset() {
            super.reset();
            checkIndexTime = 0;
        }

        public long checkIndexTime() {
            return checkIndexTime;
        }

        public void checkIndexTime(long checkIndexTime) {
            this.checkIndexTime = checkIndexTime;
        }

        public VerifyIndex(StreamInput in) throws IOException {
            super(in);
            checkIndexTime = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(checkIndexTime);
        }

    }

    public static class Translog extends Timer implements Writeable {

        public static final int UNKNOWN = -1;

        private int recovered;
        private int total;
        private int totalOnStart;
        private int totalLocal = UNKNOWN;

        public Translog() {
            total = UNKNOWN;
            totalOnStart = UNKNOWN;
        }

        public synchronized void reset() {
            super.reset();
            recovered = 0;
            total = UNKNOWN;
            totalOnStart = UNKNOWN;
            totalLocal = UNKNOWN;
        }

        public synchronized void incrementRecoveredOperations() {
            recovered++;
            assert total == UNKNOWN || total >= recovered : "total, if known, should be > recovered. total [" + total + "], recovered [" + recovered + "]";
        }

        public synchronized void incrementRecoveredOperations(int ops) {
            recovered += ops;
            assert total == UNKNOWN || total >= recovered : "total, if known, should be > recovered. total [" + total + "], recovered [" + recovered + "]";
        }

        /**
         * returns the total number of translog operations recovered so far
         */
        public synchronized int recoveredOperations() {
            return recovered;
        }

        public synchronized int totalOperationsOnStart() {
            return totalOnStart;
        }

        /**
         * returns the total number of translog operations needed to be recovered at this moment.
         * Note that this can change as the number of operations grows during recovery.
         * <p>
         * A value of -1 ({@link RecoveryState.Translog#UNKNOWN} is return if this is unknown (typically a gateway recovery)
         */
        public synchronized int totalOperations() {
            return total;
        }

        public synchronized void totalOperations(int total) {
            this.total = totalLocal == UNKNOWN ? total : totalLocal + total;
            assert total == UNKNOWN || this.total >= recovered
                : "total, if known, should be > recovered. total [" + total + "], recovered [" + recovered + "]";
        }

        public synchronized void totalOperationsOnStart(int total) {
            this.totalOnStart = totalLocal == UNKNOWN ? total : totalLocal + total;
        }

        /**
         * Sets the total number of translog operations to be recovered locally before performing peer recovery
         * @see IndexShard#recoverLocallyUpToGlobalCheckpoint()
         */
        public synchronized void totalLocal(int totalLocal) {
            assert totalLocal >= recovered : totalLocal + " < " + recovered;
            this.totalLocal = totalLocal;
        }

        public synchronized int totalLocal() {
            return totalLocal;
        }

        public synchronized float recoveredPercent() {
            if (total == UNKNOWN) {
                return -1.f;
            }
            if (total == 0) {
                return 100.f;
            }
            return recovered * 100.0f / total;
        }

        public Translog(StreamInput in) throws IOException {
            super(in);
            recovered = in.readVInt();
            total = in.readVInt();
            totalOnStart = in.readVInt();
            if (in.getVersion().onOrAfter(Version.V_4_3_0)) {
                totalLocal = in.readVInt();
            }
        }

        @Override
        @SuppressWarnings("sync-override")
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            synchronized (this) {
                out.writeVInt(recovered);
                out.writeVInt(total);
                out.writeVInt(totalOnStart);
                if (out.getVersion().onOrAfter(Version.V_4_3_0)) {
                    out.writeVInt(totalLocal);
                }
            }
        }
    }

    public static class File implements Writeable {

        private final String name;
        private final long length;
        private final boolean reused;

        private long recovered;

        public File(String name, long length, boolean reused) {
            assert name != null;
            this.name = name;
            this.length = length;
            this.reused = reused;
        }

        void addRecoveredBytes(long bytes) {
            assert reused == false : "file is marked as reused, can't update recovered bytes";
            assert bytes >= 0 : "can't recovered negative bytes. got [" + bytes + "]";
            recovered += bytes;
        }

        /**
         * file name *
         */
        public String name() {
            return name;
        }

        /**
         * file length *
         */
        public long length() {
            return length;
        }

        /**
         * number of bytes recovered for this file (so far). 0 if the file is reused *
         */
        public long recovered() {
            return recovered;
        }

        /**
         * returns true if the file is reused from a local copy
         */
        public boolean reused() {
            return reused;
        }

        boolean fullyRecovered() {
            return reused == false && length == recovered;
        }

        public File(StreamInput in) throws IOException {
            name = in.readString();
            length = in.readVLong();
            recovered = in.readVLong();
            reused = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVLong(length);
            out.writeVLong(recovered);
            out.writeBoolean(reused);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof File) {
                File other = (File) obj;
                return name.equals(other.name) && length == other.length() && reused == other.reused() && recovered == other.recovered();
            }
            return false;
        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + Long.hashCode(length);
            result = 31 * result + Long.hashCode(recovered);
            result = 31 * result + (reused ? 1 : 0);
            return result;
        }

        @Override
        public String toString() {
            return "file (name [" + name + "], reused [" + reused + "], length [" + length + "], recovered [" + recovered + "])";
        }
    }


    public static class RecoveryFilesDetails implements Writeable {
        private final Map<String, File> fileDetails = new HashMap<>();
        private boolean complete;

        RecoveryFilesDetails() {
        }

        RecoveryFilesDetails(StreamInput in) throws IOException {
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                File file = new File(in);
                fileDetails.put(file.name, file);
            }
            if (in.getVersion().onOrAfter(Version.V_5_2_0)) {
                complete = in.readBoolean();
            } else {
                // This flag is used by disk-based allocation to decide whether the remaining bytes measurement is accurate or not; if not
                // then it falls back on an estimate. There's only a very short window in which the file details are present but incomplete
                // so this is a reasonable approximation, and the stats reported to the disk-based allocator don't hit this code path
                // anyway since they always use IndexShard#getRecoveryState which is never transported over the wire.
                complete = fileDetails.isEmpty() == false;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            final File[] files = values().toArray(new File[0]);
            out.writeVInt(files.length);
            for (File file : files) {
                file.writeTo(out);
            }
            if (out.getVersion().onOrAfter(Version.V_5_2_0)) {
                out.writeBoolean(complete);
            }
        }

        public void addFileDetails(String name, long length, boolean reused) {
            assert complete == false : "addFileDetail for [" + name + "] when file details are already complete";
            File existing = fileDetails.put(name, new File(name, length, reused));
            assert existing == null : "file [" + name + "] is already reported";
        }

        public void addRecoveredBytesToFile(String name, long bytes) {
            File file = fileDetails.get(name);
            assert file != null : "file [" + name + "] hasn't been reported";
            file.addRecoveredBytes(bytes);
        }

        public File get(String name) {
            return fileDetails.get(name);
        }

        public void setComplete() {
            complete = true;
        }

        public int size() {
            return fileDetails.size();
        }

        public boolean isEmpty() {
            return fileDetails.isEmpty();
        }

        public void clear() {
            fileDetails.clear();
            complete = false;
        }

        public Collection<File> values() {
            return fileDetails.values();
        }

        public boolean isComplete() {
            return complete;
        }
    }

    public static class Index extends Timer implements Writeable {
        private final RecoveryFilesDetails fileDetails;

        public static final long UNKNOWN = -1L;

        private long sourceThrottlingInNanos = UNKNOWN;
        private long targetThrottleTimeInNanos = UNKNOWN;

        public Index() {
            super();
            this.fileDetails = new RecoveryFilesDetails();
        }

        public Index(StreamInput in) throws IOException {
            super(in);
            fileDetails = new RecoveryFilesDetails(in);
            sourceThrottlingInNanos = in.readLong();
            targetThrottleTimeInNanos = in.readLong();
        }

        @Override
        public synchronized void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            fileDetails.writeTo(out);
            out.writeLong(sourceThrottlingInNanos);
            out.writeLong(targetThrottleTimeInNanos);
        }

        public synchronized void reset() {
            super.reset();
            fileDetails.clear();
            sourceThrottlingInNanos = UNKNOWN;
            targetThrottleTimeInNanos = UNKNOWN;
        }

        public synchronized void addFileDetail(String name, long length, boolean reused) {
            fileDetails.addFileDetails(name, length, reused);
        }

        public synchronized void setFileDetailsComplete() {
            fileDetails.setComplete();
        }

        public synchronized void addRecoveredBytesToFile(String name, long bytes) {
            fileDetails.addRecoveredBytesToFile(name, bytes);
        }

        public synchronized void addSourceThrottling(long timeInNanos) {
            if (sourceThrottlingInNanos == UNKNOWN) {
                sourceThrottlingInNanos = timeInNanos;
            } else {
                sourceThrottlingInNanos += timeInNanos;
            }
        }

        public synchronized void addTargetThrottling(long timeInNanos) {
            if (targetThrottleTimeInNanos == UNKNOWN) {
                targetThrottleTimeInNanos = timeInNanos;
            } else {
                targetThrottleTimeInNanos += timeInNanos;
            }
        }

        public synchronized TimeValue sourceThrottling() {
            return TimeValue.timeValueNanos(sourceThrottlingInNanos);
        }

        public synchronized TimeValue targetThrottling() {
            return TimeValue.timeValueNanos(targetThrottleTimeInNanos);
        }

        /**
         * total number of files that are part of this recovery, both re-used and recovered
         */
        public synchronized int totalFileCount() {
            return fileDetails.size();
        }

        /**
         * total number of files to be recovered (potentially not yet done)
         */
        public synchronized int totalRecoverFiles() {
            int total = 0;
            for (File file : fileDetails.values()) {
                if (file.reused() == false) {
                    total++;
                }
            }
            return total;
        }

        /**
         * number of file that were recovered (excluding on ongoing files)
         */
        public synchronized int recoveredFileCount() {
            int count = 0;
            for (File file : fileDetails.values()) {
                if (file.fullyRecovered()) {
                    count++;
                }
            }
            return count;
        }

        /**
         * percent of recovered (i.e., not reused) files out of the total files to be recovered
         */
        public synchronized float recoveredFilesPercent() {
            int total = 0;
            int recovered = 0;
            for (File file : fileDetails.values()) {
                if (file.reused() == false) {
                    total++;
                    if (file.fullyRecovered()) {
                        recovered++;
                    }
                }
            }
            if (total == 0 && fileDetails.size() == 0) {      // indicates we are still in init phase
                return 0.0f;
            }
            if (total == recovered) {
                return 100.0f;
            } else {
                return 100.0f * (recovered / (float) total);
            }
        }

        public RecoveryFilesDetails fileDetails() {
            return fileDetails;
        }

        /**
         * total number of bytes in th shard
         */
        public synchronized long totalBytes() {
            long total = 0;
            for (File file : fileDetails.values()) {
                total += file.length();
            }
            return total;
        }

        /**
         * total number of bytes recovered so far, including both existing and reused
         */
        public synchronized long recoveredBytes() {
            long recovered = 0;
            for (File file : fileDetails.values()) {
                recovered += file.recovered();
            }
            return recovered;
        }

        /**
         * @return number of bytes still to recover, i.e. {@link Index#totalRecoverBytes()} minus {@link Index#recoveredBytes()}, or
         * {@code -1} if the full set of files to recover is not yet known
         */
        public synchronized long bytesStillToRecover() {
            if (fileDetails.isComplete() == false) {
                return -1L;
            }
            long total = 0L;
            for (File file : fileDetails.values()) {
                if (file.reused() == false) {
                    total += file.length() - file.recovered();
                }
            }
            return total;
        }

        /**
         * percent of bytes recovered out of total files bytes *to be* recovered
         */
        public synchronized float recoveredBytesPercent() {
            long total = 0;
            long recovered = 0;
            for (File file : fileDetails.values()) {
                if (file.reused() == false) {
                    total += file.length();
                    recovered += file.recovered();
                }
            }
            if (total == 0 && fileDetails.size() == 0) {
                // indicates we are still in init phase
                return 0.0f;
            }
            if (total == recovered) {
                return 100.0f;
            } else {
                return 100.0f * recovered / total;
            }
        }

        public synchronized int reusedFileCount() {
            int reused = 0;
            for (File file : fileDetails.values()) {
                if (file.reused()) {
                    reused++;
                }
            }
            return reused;
        }

        public synchronized long reusedBytes() {
            long reused = 0;
            for (File file : fileDetails.values()) {
                if (file.reused()) {
                    reused += file.length();
                }
            }
            return reused;
        }

        public File getFileDetails(String dest) {
            return fileDetails.get(dest);
        }
    }
}
