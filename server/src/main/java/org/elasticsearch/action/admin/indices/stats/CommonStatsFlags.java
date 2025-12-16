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

package org.elasticsearch.action.admin.indices.stats;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

public class CommonStatsFlags implements Writeable, Cloneable {

    public static final CommonStatsFlags ALL = new CommonStatsFlags().all();
    public static final CommonStatsFlags NONE = new CommonStatsFlags().clear();

    private EnumSet<Flag> flags = EnumSet.allOf(Flag.class);
    private String[] groups = null;
    private String[] fieldDataFields = null;
    private String[] completionDataFields = null;
    private boolean includeSegmentFileSizes = false;

    /**
     * @param flags flags to set. If no flags are supplied, default flags will be set.
     */
    public CommonStatsFlags(Flag... flags) {
        if (flags.length > 0) {
            clear();
            Collections.addAll(this.flags, flags);
        }
    }

    public CommonStatsFlags(StreamInput in) throws IOException {
        final long longFlags = in.readLong();
        flags.clear();
        for (Flag flag : Flag.values()) {
            if ((longFlags & (1 << flag.getIndex())) != 0) {
                flags.add(flag);
            }
        }
        groups = in.readStringArray();
        fieldDataFields = in.readStringArray();
        completionDataFields = in.readStringArray();
        includeSegmentFileSizes = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        long longFlags = 0;
        for (Flag flag : flags) {
            longFlags |= (1 << flag.getIndex());
        }
        out.writeLong(longFlags);

        out.writeStringArrayNullable(groups);
        out.writeStringArrayNullable(fieldDataFields);
        out.writeStringArrayNullable(completionDataFields);
        out.writeBoolean(includeSegmentFileSizes);
    }

    /**
     * Sets all flags to return all stats.
     */
    public CommonStatsFlags all() {
        flags = EnumSet.allOf(Flag.class);
        groups = null;
        fieldDataFields = null;
        completionDataFields = null;
        includeSegmentFileSizes = false;
        return this;
    }

    /**
     * Clears all stats.
     */
    public CommonStatsFlags clear() {
        flags = EnumSet.noneOf(Flag.class);
        groups = null;
        fieldDataFields = null;
        completionDataFields = null;
        includeSegmentFileSizes = false;
        return this;
    }

    public boolean anySet() {
        return !flags.isEmpty();
    }

    public Flag[] getFlags() {
        return flags.toArray(new Flag[flags.size()]);
    }

    /**
     * Sets specific search group stats to retrieve the stats for. Mainly affects search
     * when enabled.
     */
    public CommonStatsFlags groups(String... groups) {
        this.groups = groups;
        return this;
    }

    public String[] groups() {
        return this.groups;
    }

    /**
     * Sets specific search group stats to retrieve the stats for. Mainly affects search
     * when enabled.
     */
    public CommonStatsFlags fieldDataFields(String... fieldDataFields) {
        this.fieldDataFields = fieldDataFields;
        return this;
    }

    public String[] fieldDataFields() {
        return this.fieldDataFields;
    }

    public CommonStatsFlags completionDataFields(String... completionDataFields) {
        this.completionDataFields = completionDataFields;
        return this;
    }

    public String[] completionDataFields() {
        return this.completionDataFields;
    }

    public CommonStatsFlags includeSegmentFileSizes(boolean includeSegmentFileSizes) {
        this.includeSegmentFileSizes = includeSegmentFileSizes;
        return this;
    }

    public boolean includeSegmentFileSizes() {
        return this.includeSegmentFileSizes;
    }

    public boolean isSet(Flag flag) {
        return flags.contains(flag);
    }

    boolean unSet(Flag flag) {
        return flags.remove(flag);
    }

    void set(Flag flag) {
        flags.add(flag);
    }

    public CommonStatsFlags set(Flag flag, boolean add) {
        if (add) {
            set(flag);
        } else {
            unSet(flag);
        }
        return this;
    }

    @Override
    public CommonStatsFlags clone() {
        try {
            CommonStatsFlags cloned = (CommonStatsFlags) super.clone();
            cloned.flags = flags.clone();
            return cloned;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    public enum Flag {
        Store(0),
        Docs(9);

        private final int index;

        Flag(final int index) {
            this.index = index;
        }

        private int getIndex() {
            return index;
        }
    }
}
