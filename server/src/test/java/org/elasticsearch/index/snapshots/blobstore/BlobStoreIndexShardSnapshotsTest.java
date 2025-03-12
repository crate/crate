/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package org.elasticsearch.index.snapshots.blobstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.INDEX_SHARD_SNAPSHOTS_FORMAT;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import com.sun.management.ThreadMXBean;

public class BlobStoreIndexShardSnapshotsTest extends ESTestCase {

    @Test
    public void test_bwc_streaming() throws Exception {
        BlobStoreIndexShardSnapshots blobStoreIndexShardSnapshots = prepareData();
        BytesStreamOutput out = new BytesStreamOutput();
        blobStoreIndexShardSnapshots.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        BlobStoreIndexShardSnapshots fromStream = BlobStoreIndexShardSnapshots.fromStream(in);
        assertThat(isSame(blobStoreIndexShardSnapshots, fromStream)).isTrue();

        out = new BytesStreamOutput();
        out.setVersion(Version.V_5_10_1);
        blobStoreIndexShardSnapshots.writeTo(out);

        in = out.bytes().streamInput();
        in.setVersion(org.elasticsearch.Version.V_5_10_1);
        fromStream = BlobStoreIndexShardSnapshots.fromStream(in);
        assertThat(isSame(blobStoreIndexShardSnapshots, fromStream)).isTrue();

    }

    @Test
    public void test_read_from_stream_allocations() throws Exception {
        BytesReference bytesReference = INDEX_SHARD_SNAPSHOTS_FORMAT.serialize(prepareData(), "dummyBlobName", true);

        ThreadMXBean threadMXBean = ManagementFactory.getPlatformMXBean(ThreadMXBean.class);
        long threadId = Thread.currentThread().threadId();
        long allocatedBytesBegin = threadMXBean.getThreadAllocatedBytes(threadId);

        INDEX_SHARD_SNAPSHOTS_FORMAT.deserialize(
            "dummyBlobname",
            writableRegistry(),
            xContentRegistry(),
            bytesReference
        );

        long allocatedBytesAfter = threadMXBean.getThreadAllocatedBytes(threadId);
        long allocatedBytes = allocatedBytesAfter - allocatedBytesBegin;
        // Actual value fluctuates around 9_877_672.
        // Used to fluctuate around 29_228_160 on 5.10.2 when instances were not re-used.
        assertThat(allocatedBytes).isLessThanOrEqualTo(10_000_000L);
    }

    /**
     * Returns an instance with SnapshotFiles instances having their indexFiles overlapping
     * and those overlaps being re-used as a same instance.
     *
     * We need to ensure that we can read such optimal file in a way that deserialized instance also has an optimal structure.
     */
    private static BlobStoreIndexShardSnapshots prepareData() throws Exception {
        // Method getThreadAllocatedBytes is a rough estimation and returned value fluctuates a little bit.
        // Generate a bigger list so that difference with/without the fix is visible (like 3x) and can't be treated as a noise.
        List<FileInfo> fileInfos = new ArrayList<>();
        for (int i = 1; i <= 10_000; i++) {
            String name = "name" + i;
            fileInfos.add(
                new FileInfo(
                    name,
                    new StoreFileMetadata(name, 1, "dummy_checksum", org.apache.lucene.util.Version.LUCENE_9_12_0),
                    new ByteSizeValue(i)
                )
            );
        }

        // Different SnapshotFiles can refer to the same FileInfo-s in the SnapshotFiles.indexFiles field.
        // Making 2 lists almost completely overlapping to make difference visible and eliminate getThreadAllocatedBytes fluctuation noise.
        SnapshotFiles snapshot1 = new SnapshotFiles("1", fileInfos.subList(10, fileInfos.size()), null);
        SnapshotFiles snapshot2 = new SnapshotFiles("1", fileInfos.subList(0, fileInfos.size() - 10), null);
        return new BlobStoreIndexShardSnapshots(List.of(snapshot1, snapshot2));
    }

    private static boolean isSame(BlobStoreIndexShardSnapshots shardSnapshot1, BlobStoreIndexShardSnapshots shardSnapshot2) {
        Map<String, FileInfo> files1 = shardSnapshot1.files();
        Map<String, FileInfo> files2 = shardSnapshot1.files();
        if (files1.size() != files2.size()) {
            return false;
        }
        for (Map.Entry<String, FileInfo> entry : files1.entrySet()) {
            if (entry.getValue().isSame(files2.get(entry.getKey())) == false) {
                return false;
            }
        }

        Map<String, List<FileInfo>> physicalFiles1 = shardSnapshot1.physicalFiles();
        Map<String, List<FileInfo>> physicalFiles2 = shardSnapshot1.physicalFiles();
        if (physicalFiles1.size() != physicalFiles2.size()) {
            return false;
        }
        for (Map.Entry<String, List<FileInfo>> entry : physicalFiles1.entrySet()) {
            List<FileInfo> list1 = entry.getValue();
            List<FileInfo> list2 = physicalFiles2.get(entry.getKey());
            if (list1.size() != list2.size()) {
                return false;
            }
            for (int i = 0; i < list1.size(); i++) {
                if (list1.get(i).isSame(list2.get(i)) == false) {
                    return false;
                }
            }
        }

        List<SnapshotFiles> shapshotFiles1 = shardSnapshot1.snapshots();
        List<SnapshotFiles> shapshotFiles2 = shardSnapshot1.snapshots();
        if (shapshotFiles1.size() != shapshotFiles2.size()) {
            return false;
        }
        for (int i = 0; i < shapshotFiles1.size(); i++) {
            SnapshotFiles sf1 = shapshotFiles1.get(i);
            SnapshotFiles sf2 = shapshotFiles2.get(i);
            if (sf1.snapshot().equals(sf2.snapshot()) == false) {
                return false;
            }
            if (Objects.equals(sf1.shardStateIdentifier(), sf2.shardStateIdentifier()) == false) {
                return false;
            }
            List<FileInfo> list1 = sf1.indexFiles();
            List<FileInfo> list2 = sf2.indexFiles();
            if (list1.size() != list2.size()) {
                return false;
            }
            for (int j = 0; j < list1.size(); j++) {
                if (list1.get(j).isSame(list2.get(j)) == false) {
                    return false;
                }
            }
        }
        return true;
    }
}
