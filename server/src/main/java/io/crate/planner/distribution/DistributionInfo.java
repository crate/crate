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

package io.crate.planner.distribution;

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

public class DistributionInfo implements Writeable {

    public static final DistributionInfo DEFAULT_BROADCAST = new DistributionInfo(DistributionType.BROADCAST);
    public static final DistributionInfo DEFAULT_SAME_NODE = new DistributionInfo(DistributionType.SAME_NODE);
    public static final DistributionInfo DEFAULT_MODULO = new DistributionInfo(DistributionType.MODULO);

    private final DistributionType distributionType;
    private final int distributeByColumn;

    public DistributionInfo(DistributionType distributionType, int distributeByColumn) {
        this.distributionType = distributionType;
        this.distributeByColumn = distributeByColumn;
    }

    public DistributionInfo(StreamInput in) throws IOException {
        distributionType = DistributionType.values()[in.readVInt()];
        distributeByColumn = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(distributionType.ordinal());
        out.writeVInt(distributeByColumn);
    }

    public DistributionInfo(DistributionType distributionType) {
        this(distributionType, 0);
    }

    public DistributionType distributionType() {
        return distributionType;
    }

    public int distributeByColumn() {
        return distributeByColumn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DistributionInfo that = (DistributionInfo) o;

        return distributeByColumn == that.distributeByColumn && distributionType == that.distributionType;
    }

    @Override
    public int hashCode() {
        int result = distributionType.hashCode();
        result = 31 * result + distributeByColumn;
        return result;
    }

    @Override
    public String toString() {
        return "DistributionInfo{" +
               "distributionType=" + distributionType +
               ", distributeByColumn=" + distributeByColumn +
               '}';
    }
}
