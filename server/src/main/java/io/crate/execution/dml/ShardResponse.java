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

package io.crate.execution.dml;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.jetbrains.annotations.Nullable;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.Streamer;
import io.crate.execution.dml.upsert.ShardUpsertRequest;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;

public class ShardResponse extends ReplicationResponse implements WriteResponse {

    /**
     * Represents a failure.
     */
    public static class Failure implements Writeable {

        private final String id;
        private final String message;
        private final boolean versionConflict;

        public Failure(String id, String message, boolean versionConflict) {
            this.id = id;
            this.message = message;
            this.versionConflict = versionConflict;
        }

        public Failure(StreamInput in) throws IOException {
            id = in.readString();
            message = in.readString();
            versionConflict = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
            out.writeString(message);
            out.writeBoolean(versionConflict);
        }

        public String id() {
            return id;
        }

        public String message() {
            return this.message;
        }

        public boolean versionConflict() {
            return versionConflict;
        }

        @Override
        public String toString() {
            return "Failure{" +
                   "id='" + id + '\'' +
                   ", message='" + message + '\'' +
                   ", versionConflict=" + versionConflict +
                   '}';
        }
    }

    private IntArrayList locations = new IntArrayList();
    private List<Failure> failures = new ArrayList<>();

    /**
     * Result rows are used to return values from updated rows.
     */
    @Nullable
    private List<Object[]> resultRows;

    /**
     * Result columns describe the types of the result rows.
     */
    @Nullable
    private Symbol[] resultColumns;


    @Nullable
    private Exception failure;


    public ShardResponse() {
    }

    public ShardResponse(@Nullable Symbol[] resultColumns) {
        this.resultColumns = resultColumns;
    }

    public void add(int location) {
        locations.add(location);
        failures.add(null);
    }

    public void add(int location, Failure failure) {
        locations.add(location);
        failures.add(failure);
    }

    public void addResultRows(Object[] rows) {
        if (resultRows == null) {
            resultRows = new ArrayList<>();
        }
        resultRows.add(rows);
    }

    @Nullable
    public List<Object[]> getResultRows() {
        return resultRows;
    }

    public IntArrayList itemIndices() {
        return locations;
    }

    public List<Failure> failures() {
        return failures;
    }

    public void failure(@Nullable Exception failure) {
        this.failure = failure;
    }

    public Exception failure() {
        return failure;
    }

    public ShardResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        locations = new IntArrayList(size);
        failures = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            locations.add(in.readVInt());
            if (in.readBoolean()) {
                failures.add(new Failure(in));
            } else {
                failures.add(null);
            }
        }
        if (in.readBoolean()) {
            failure = in.readException();
        }
        if (in.getVersion().onOrAfter(Version.V_4_2_0)) {
            int resultColumnsSize = in.readVInt();
            if (resultColumnsSize > 0) {
                resultColumns = new Symbol[resultColumnsSize];
                for (int i = 0; i < resultColumnsSize; i++) {
                    Symbol symbol = Symbols.fromStream(in);
                    resultColumns[i] = symbol;
                }
                Streamer<?>[] resultRowStreamers = Symbols.streamerArray(resultColumns);
                int resultRowsSize = in.readVInt();
                if (resultRowsSize > 0) {
                    resultRows = new ArrayList<>(resultRowsSize);
                    int rowLength = in.readVInt();
                    for (int i = 0; i < resultRowsSize; i++) {
                        Object[] row = new Object[rowLength];
                        for (int j = 0; j < rowLength; j++) {
                            row[j] = resultRowStreamers[j].readValueFrom(in);
                        }
                        resultRows.add(row);
                    }
                }
            }
        }
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(locations.size());
        for (int i = 0; i < locations.size(); i++) {
            out.writeVInt(locations.get(i));
            if (failures.get(i) == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                failures.get(i).writeTo(out);
            }
        }
        if (failure != null) {
            out.writeBoolean(true);
            out.writeException(failure);
        } else {
            out.writeBoolean(false);
        }
        if (out.getVersion().onOrAfter(Version.V_4_2_0)) {
            if (resultRows != null) {
                assert resultColumns != null : "Result columns are required when writing result rows";
                Streamer[] resultRowStreamers = Symbols.streamerArray(resultColumns);
                out.writeVInt(resultColumns.length);
                for (int i = 0; i < resultColumns.length; i++) {
                    Symbols.toStream(resultColumns[i], out);
                }
                out.writeVInt(resultRows.size());
                int rowLength = resultRows.get(0).length;
                out.writeVInt(rowLength);
                for (Object[] row : resultRows) {
                    for (int j = 0; j < rowLength; j++) {
                        resultRowStreamers[j].writeValueTo(out, row[j]);
                    }
                }
            } else {
                out.writeVInt(0);
            }
        }
    }


    /**
     * The result in compressed form.
     * <p>
     * It contains the locations that were successful(=write happened) and the locations that failed.
     * </p>
     *
     * <p>
     * It can also be possible that items were ignored (`ON CONFLICT (pk) DO NOTHING`),
     * in this case, both `successfulWrites` and `failed` will be false.
     * </p>
     *
     * This doesn't contain failure reasons.
     */
    public static class CompressedResult {

        private final BitSet successfulWrites = new BitSet();
        private final BitSet failureLocations = new BitSet();
        private final ArrayList<Object[]> resultRows = new ArrayList<>();

        public void update(ShardResponse response) {
            IntArrayList itemIndices = response.itemIndices();
            List<Failure> failures = response.failures();
            for (int i = 0; i < itemIndices.size(); i++) {
                int location = itemIndices.get(i);
                ShardResponse.Failure failure = failures.get(i);
                if (failure == null) {
                    successfulWrites.set(location, true);
                } else {
                    failureLocations.set(location, true);
                }
            }
            List<Object[]> resultRows = response.getResultRows();
            if (resultRows != null) {
                this.resultRows.addAll(resultRows);
            }
        }

        public boolean successfulWrites(int location) {
            return successfulWrites.get(location);
        }

        public boolean failed(int location) {
            return failureLocations.get(location);
        }

        public List<Object[]> resultRows() {
            return resultRows;
        }

        public int numSuccessfulWrites() {
            return successfulWrites.cardinality();
        }

        public void markAsFailed(List<ShardUpsertRequest.Item> items) {
            for (ShardUpsertRequest.Item item : items) {
                failureLocations.set(item.location());
            }
        }
    }

    public int successRowCount() {
        int numSuccessful = 0;
        for (int i = 0; i < locations.size(); i++) {
            Failure failure = failures.get(i);
            if (failure == null) {
                numSuccessful++;
            }
        }
        return numSuccessful;
    }
}
