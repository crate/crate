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
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class Segment implements Streamable {

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

    Segment() {
    }

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
     * If set, a string representing that the segment is part of a merge, with the value representing the
     * group of segments that represent this merge.
     */
    @Nullable
    public String getMergeId() {
        return this.mergeId;
    }

    /**
     * Estimation of the memory usage used by a segment.
     */
    public long getMemoryInBytes() {
        return this.memoryInBytes;
    }

    /**
     * Return the sort order of this segment, or null if the segment has no sort.
     */
    public Sort getSegmentSort() {
        return segmentSort;
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

    public static Segment readSegment(StreamInput in) throws IOException {
        Segment segment = new Segment();
        segment.readFrom(in);
        return segment;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        generation = Long.parseLong(name.substring(1), Character.MAX_RADIX);
        committed = in.readBoolean();
        search = in.readBoolean();
        docCount = in.readInt();
        delDocCount = in.readInt();
        sizeInBytes = in.readLong();
        version = Lucene.parseVersionLenient(in.readOptionalString(), null);
        compound = in.readOptionalBoolean();
        mergeId = in.readOptionalString();
        memoryInBytes = in.readLong();
        if (in.readBoolean()) {
            // verbose mode
            ramTree = readRamTree(in);
        }
        if (in.getVersion().onOrAfter(Version.V_6_0_0_alpha1)) {
            segmentSort = readSegmentSort(in);
        } else {
            segmentSort = null;
        }
        if (in.getVersion().onOrAfter(Version.V_6_1_0) && in.readBoolean()) {
            attributes = in.readMap(StreamInput::readString, StreamInput::readString);
        } else {
            attributes = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeBoolean(committed);
        out.writeBoolean(search);
        out.writeInt(docCount);
        out.writeInt(delDocCount);
        out.writeLong(sizeInBytes);
        out.writeOptionalString(version.toString());
        out.writeOptionalBoolean(compound);
        out.writeOptionalString(mergeId);
        out.writeLong(memoryInBytes);

        boolean verbose = ramTree != null;
        out.writeBoolean(verbose);
        if (verbose) {
            writeRamTree(out, ramTree);
        }
        if (out.getVersion().onOrAfter(Version.V_6_0_0_alpha1)) {
            writeSegmentSort(out, segmentSort);
        }
        if (out.getVersion().onOrAfter(Version.V_6_1_0)) {
            boolean hasAttributes = attributes != null;
            out.writeBoolean(hasAttributes);
            if (hasAttributes) {
                out.writeMap(attributes, StreamOutput::writeString, StreamOutput::writeString);
            }
        }
    }

    Sort readSegmentSort(StreamInput in) throws IOException {
        int size = in.readVInt();
        if (size == 0) {
            return null;
        }
        SortField[] fields = new SortField[size];
        for (int i = 0; i < size; i++) {
            String field = in.readString();
            byte type = in.readByte();
            if (type == 0) {
                Boolean missingFirst = in.readOptionalBoolean();
                boolean max = in.readBoolean();
                boolean reverse = in.readBoolean();
                fields[i] = new SortedSetSortField(field, reverse,
                    max ? SortedSetSelector.Type.MAX : SortedSetSelector.Type.MIN);
                if (missingFirst != null) {
                    fields[i].setMissingValue(missingFirst ?
                        SortedSetSortField.STRING_FIRST : SortedSetSortField.STRING_LAST);
                }
            } else {
                Object missing = in.readGenericValue();
                boolean max = in.readBoolean();
                boolean reverse = in.readBoolean();
                final SortField.Type numericType;
                switch (type) {
                    case 1:
                        numericType = SortField.Type.INT;
                        break;
                    case 2:
                        numericType = SortField.Type.FLOAT;
                        break;
                    case 3:
                        numericType = SortField.Type.DOUBLE;
                        break;
                    case 4:
                        numericType = SortField.Type.LONG;
                        break;
                    default:
                        throw new IOException("invalid index sort type:[" + type +
                            "] for numeric field:[" + field + "]");
                }
                fields[i] = new SortedNumericSortField(field, numericType, reverse, max ?
                    SortedNumericSelector.Type.MAX : SortedNumericSelector.Type.MIN);
                if (missing != null) {
                    fields[i].setMissingValue(missing);
                }
            }
        }
        return new Sort(fields);
    }

    void writeSegmentSort(StreamOutput out, Sort sort) throws IOException {
        if (sort == null) {
            out.writeVInt(0);
            return;
        }
        out.writeVInt(sort.getSort().length);
        for (SortField field : sort.getSort()) {
            out.writeString(field.getField());
            if (field instanceof SortedSetSortField) {
                out.writeByte((byte) 0);
                out.writeOptionalBoolean(field.getMissingValue() == null ?
                    null : field.getMissingValue() == SortField.STRING_FIRST);
                out.writeBoolean(((SortedSetSortField) field).getSelector() == SortedSetSelector.Type.MAX);
                out.writeBoolean(field.getReverse());
            } else if (field instanceof SortedNumericSortField) {
                switch (((SortedNumericSortField) field).getNumericType()) {
                    case INT:
                        out.writeByte((byte) 1);
                        break;
                    case FLOAT:
                        out.writeByte((byte) 2);
                        break;
                    case DOUBLE:
                        out.writeByte((byte) 3);
                        break;
                    case LONG:
                        out.writeByte((byte) 4);
                        break;
                    default:
                        throw new IOException("invalid index sort field:" + field);
                }
                out.writeGenericValue(field.getMissingValue());
                out.writeBoolean(((SortedNumericSortField) field).getSelector() == SortedNumericSelector.Type.MAX);
                out.writeBoolean(field.getReverse());
            } else {
                throw new IOException("invalid index sort field:" + field);
            }
        }
    }

    Accountable readRamTree(StreamInput in) throws IOException {
        final String name = in.readString();
        final long bytes = in.readVLong();
        int numChildren = in.readVInt();
        if (numChildren == 0) {
            return Accountables.namedAccountable(name, bytes);
        }
        List<Accountable> children = new ArrayList(numChildren);
        while (numChildren-- > 0) {
            children.add(readRamTree(in));
        }
        return Accountables.namedAccountable(name, children, bytes);
    }

    // the ram tree is written recursively since the depth is fairly low (5 or 6)
    void writeRamTree(StreamOutput out, Accountable tree) throws IOException {
        out.writeString(tree.toString());
        out.writeVLong(tree.ramBytesUsed());
        Collection<Accountable> children = tree.getChildResources();
        out.writeVInt(children.size());
        for (Accountable child : children) {
            writeRamTree(out, child);
        }
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
