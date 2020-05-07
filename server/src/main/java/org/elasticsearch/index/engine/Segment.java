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

package org.elasticsearch.index.engine;

import org.apache.lucene.search.Sort;
import org.apache.lucene.util.Accountable;
import javax.annotation.Nullable;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.Map;

public class Segment {

    private String name;
    private long generation;
    public boolean committed;
    public boolean search;
    public long sizeInBytes = -1;
    public int docCount = -1;
    public int delDocCount = -1;
    public org.apache.lucene.util.Version version = null;
    public Boolean compound = null;
    public String mergeId;
    public long memoryInBytes;
    public Sort segmentSort;
    public Accountable ramTree = null;
    public Map<String, String> attributes;

    public Segment(String name) {
        this.name = name;
        this.generation = Long.parseLong(name.substring(1), Character.MAX_RADIX);
    }

    public String getName() {
        return this.name;
    }

    public long getGeneration() {
        return this.generation;
    }

    public boolean isCommitted() {
        return this.committed;
    }

    public boolean isSearch() {
        return this.search;
    }

    public int getNumDocs() {
        return this.docCount;
    }

    public int getDeletedDocs() {
        return this.delDocCount;
    }

    public ByteSizeValue getSize() {
        return new ByteSizeValue(sizeInBytes);
    }

    /**
     * If set, a string representing that the segment is part of a merge, with the value representing the
     * group of segments that represent this merge.
     */
    @Nullable
    public String getMergeId() {
        return this.mergeId;
    }

    public long getSizeInBytes() {
        return this.sizeInBytes;
    }

    public org.apache.lucene.util.Version getVersion() {
        return version;
    }

    @Nullable
    public Boolean isCompound() {
        return compound;
    }

    /**
     * Return segment attributes.
     * @see org.apache.lucene.index.SegmentInfo#getAttributes()
     */
    public Map<String, String> getAttributes() {
        return attributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Segment segment = (Segment) o;

        if (name != null ? !name.equals(segment.name) : segment.name != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "Segment{" +
                "name='" + name + '\'' +
                ", generation=" + generation +
                ", committed=" + committed +
                ", search=" + search +
                ", sizeInBytes=" + sizeInBytes +
                ", docCount=" + docCount +
                ", delDocCount=" + delDocCount +
                ", version='" + version + '\'' +
                ", compound=" + compound +
                ", mergeId='" + mergeId + '\'' +
                ", memoryInBytes=" + memoryInBytes +
                (segmentSort != null ? ", sort=" + segmentSort : "") +
                ", attributes=" + attributes +
                '}';
    }
}
