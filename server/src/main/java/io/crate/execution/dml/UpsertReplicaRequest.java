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
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;

import io.crate.Streamer;
import io.crate.common.collections.Lists;
import io.crate.execution.dml.upsert.ShardUpsertRequest;
import io.crate.execution.dml.upsert.ShardUpsertRequest.DuplicateKeyAction;
import io.crate.metadata.Reference;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.SessionSettings;
import io.crate.types.DataTypes;

public class UpsertReplicaRequest extends ShardRequest<UpsertReplicaRequest, UpsertReplicaRequest.Item> {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(UpsertReplicaRequest.class);

    private final List<Reference> columns;
    private final SessionSettings sessionSettings;

    public UpsertReplicaRequest(ShardId shardId,
                                UUID jobId,
                                SessionSettings sessionSettings,
                                List<Reference> columns,
                                List<UpsertReplicaRequest.Item> items) {
        super(shardId, jobId);
        this.sessionSettings = sessionSettings;
        this.columns = columns;
        this.items = items;
    }

    public static UpsertReplicaRequest readFrom(Schemas schemas, StreamInput in) throws IOException {
        if (in.getVersion().onOrBefore(Version.V_5_10_10)) {
            ShardUpsertRequest request = new ShardUpsertRequest(schemas, in);
            return new UpsertReplicaRequest(
                request.shardId(),
                request.jobId(),
                request.sessionSettings(),
                Arrays.asList(request.insertColumns()),
                Lists.mapLazy(request.items(), UpsertReplicaRequest.Item::of)
            );
        } else {
            return new UpsertReplicaRequest(schemas, in);
        }
    }

    private UpsertReplicaRequest(Schemas schemas, StreamInput in) throws IOException {
        super(in);
        this.sessionSettings = new SessionSettings(in);
        DocTableInfo table = schemas.getTableInfo(shardId.getIndex());
        if (in.getVersion().onOrAfter(Version.V_6_2_0)) {
            int numColumns = in.readVInt();
            this.columns = new ArrayList<>(numColumns);
            for (int i = 0; i < numColumns; i++) {
                long oid = in.readLong();
                Reference column;
                boolean readAndAdjustValueType = false;
                if (oid == Metadata.COLUMN_OID_UNASSIGNED) {
                    column = Reference.fromStream(in);
                } else {
                    column = table.getReference(oid);
                    // The value type used for streaming must be used instead of the registered type as it may differ
                    // (e.g. for dynamic references where the type is adjusted in-between).
                    readAndAdjustValueType = true;
                }
                if (column == null) {
                    throw new IllegalStateException("Reference with oid=" + oid + " not found");
                }
                if (readAndAdjustValueType) {
                    column = column.withValueType(DataTypes.fromStream(in));
                }
                columns.add(column);
            }
        } else {
            this.columns = in.readList(Reference::fromStream);
        }
        int numItems = in.readVInt();
        this.items = new ArrayList<>(numItems);
        for (int i = 0; i < numItems; i++) {
            items.add(new Item(in, columns));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrBefore(Version.V_5_10_10)) {
            Reference[] insertColumns = columns.toArray(Reference[]::new);
            ShardUpsertRequest shardUpsertRequest = new ShardUpsertRequest(
                shardId,
                jobId(),
                true, // continueOnError is not relevant for replica
                DuplicateKeyAction.IGNORE, // not relevant for replica
                sessionSettings,
                null, // updateColumns are only used on primary via UpdateToInsert logic
                insertColumns,
                null // return values are only used on primary
            );
            for (int i = 0; i < items.size(); i++) {
                var item = items.get(i);
                var primaryItem = ShardUpsertRequest.Item.forInsert(
                    item.id(),
                    item.pkValues(),
                    Translog.UNSET_AUTO_GENERATED_TIMESTAMP, // not used on replica
                    insertColumns,
                    item.insertValues(),
                    null,
                    0L
                );
                primaryItem.seqNo = item.seqNo();
                primaryItem.primaryTerm = item.primaryTerm();
                primaryItem.version = item.version();
                shardUpsertRequest.add(i, primaryItem);
            }
            shardUpsertRequest.writeTo(out);
            return;
        }
        super.writeTo(out);
        sessionSettings.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_6_2_0)) {
            out.writeVInt(columns.size());
            for (Reference column : columns) {
                out.writeLong(column.oid());
                if (column.oid() == Metadata.COLUMN_OID_UNASSIGNED) {
                    Reference.toStream(out, column);
                } else {
                    // We must write the value type as well, because the reference's registered type may differ from
                    // the value type used for the streaming of the value (e.g. for dynamic references).
                    DataTypes.toStream(column.valueType(), out);
                }
            }
        } else {
            out.writeCollection(columns, Reference::toStream);
        }
        out.writeVInt(items.size());
        for (Item items : items) {
            items.writeTo(out, columns);
        }
    }

    public List<Reference> columns() {
        return columns;
    }

    public SessionSettings sessionSettings() {
        return sessionSettings;
    }

    /// Item for:
    /// - INSERT INTO (...) [ ON CONFLICT (...) DO UPDATE SET .. ]
    /// - UPDATE TABLE ... SET ...
    ///
    /// `values` can have a different length per row depending on if the ON CONFLICT clause was hit.
    /// E.g. in:
    ///
    /// ```
    /// INSERT INTO tbl (id, x) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET y = 10
    /// ```
    ///
    /// - `request.columns` will contain `[id, x, y]`
    /// - `item.values` can have 2 or 3 items
    ///
    /// @see [io.crate.execution.dml.upsert.UpdatetoInsert]
    public static class Item extends ShardRequest.Item implements IndexItem {

        private final List<String> pkValues;
        private final Object[] values;

        public Item(String id,
                    Object[] values,
                    List<String> pkValues,
                    long seqNo,
                    long primaryTerm,
                    long version) {
            super(id);
            this.values = values;
            this.pkValues = pkValues;
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
            this.version = version;
        }

        /**
         * @deprecated for BWC, dealing with requests from < 5.10.11
         **/
        @Deprecated
        public static Item of(ShardUpsertRequest.Item item) {
            return new Item(
                item.id(),
                item.insertValues(),
                item.pkValues(),
                item.seqNo(),
                item.primaryTerm(),
                item.version()
            );
        }

        public Item(StreamInput in, List<Reference> columns) throws IOException {
            super(in);
            int numValues = in.readVInt();
            // numValues can be < columns, due to ON CONFLICT
            values = new Object[numValues];
            for (int i = 0; i < numValues; i++) {
                Streamer<?> streamer = columns.get(i).valueType().streamer();
                values[i] = streamer.readValueFrom(in);
            }
            pkValues = in.readStringList();
        }

        @SuppressWarnings("unchecked")
        public void writeTo(StreamOutput out, List<Reference> columns) throws IOException {
            super.writeTo(out);
            out.writeVInt(values.length);
            for (int i = 0; i < values.length; i++) {
                Streamer<Object> streamer = (Streamer<Object>) columns.get(i).valueType().streamer();
                Object v = values[i];
                streamer.writeValueTo(out, v);
            }
            out.writeStringCollection(pkValues);
        }

        @Override
        public Object[] insertValues() {
            return values;
        }

        public List<String> pkValues() {
            return pkValues;
        }

        public long seqNo() {
            return seqNo;
        }

        public long primaryTerm() {
            return primaryTerm;
        }

        public long version() {
            return version;
        }
    }

    @Override
    protected long shallowSize() {
        return SHALLOW_SIZE;
    }
}
