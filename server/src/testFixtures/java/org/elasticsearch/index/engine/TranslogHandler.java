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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;

import io.crate.execution.dml.TranslogIndexer;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.StringType;

public class TranslogHandler implements Engine.TranslogRecoveryRunner {

    private final String indexName;
    private final TranslogIndexer indexer;

    public TranslogHandler(IndexSettings indexSettings) {
        this.indexName = indexSettings.getIndex().getName();
        this.indexer = translogIndexer(indexName, indexSettings);
    }

    private static TranslogIndexer translogIndexer(String indexName, IndexSettings indexSettings) {
        RelationName relation = RelationName.fromIndexName(indexName);
        Reference column = new SimpleReference(
            new ReferenceIdent(relation, "value"),
            RowGranularity.DOC,
            StringType.INSTANCE,
            IndexType.PLAIN,
            false,
            true,
            0,
            Metadata.COLUMN_OID_UNASSIGNED,
            false,
            null);
        DocTableInfo table = new DocTableInfo(
            relation,
            Map.of(ColumnIdent.of("value"), column),
            Map.of(),
            null,
            List.of(),
            List.of(),
            null,
            indexSettings.getSettings(),
            List.of(),
            ColumnPolicy.DYNAMIC,
            Version.CURRENT,
            Version.CURRENT,
            false,
            Set.of(),
            0
        );
        return new TranslogIndexer(table);
    }

    private void applyOperation(Engine engine, Engine.Operation operation) throws IOException {
        switch (operation.operationType()) {
            case INDEX:
                engine.index((Engine.Index) operation);
                break;
            case DELETE:
                engine.delete((Engine.Delete) operation);
                break;
            case NO_OP:
                engine.noOp((Engine.NoOp) operation);
                break;
            default:
                throw new IllegalStateException("No operation defined for [" + operation + "]");
        }
    }

    @Override
    public int run(Engine engine, Translog.Snapshot snapshot) throws IOException {
        int opsRecovered = 0;
        Translog.Operation operation;
        while ((operation = snapshot.next()) != null) {
            applyOperation(engine, convertToEngineOp(operation));
            opsRecovered++;
        }
        engine.syncTranslog();
        return opsRecovered;
    }

    private Engine.Operation convertToEngineOp(Translog.Operation operation) {
        return switch (operation.opType()) {
            case INDEX -> {
                final Translog.Index index = (Translog.Index) operation;
                yield IndexShard.prepareIndex(
                    indexer,
                    new SourceToParse(
                        indexName,
                        index.id(),
                        index.getSource(),
                        XContentType.JSON
                    ),
                    index.seqNo(),
                    index.primaryTerm(),
                    index.version(),
                    null,
                    Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY,
                    index.getAutoGeneratedIdTimestamp(),
                    true,
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    0
                );
            }
            case DELETE -> {
                final Translog.Delete delete = (Translog.Delete) operation;
                yield new Engine.Delete(
                    delete.id(),
                    delete.uid(),
                    delete.seqNo(),
                    delete.primaryTerm(),
                    delete.version(),
                    null,
                    Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY,
                    System.nanoTime(),
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    0
                );
            }
            case NO_OP -> {
                final Translog.NoOp noOp = (Translog.NoOp) operation;
                yield new Engine.NoOp(noOp.seqNo(), noOp.primaryTerm(), Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY, System.nanoTime(), noOp.reason());
            }
            default -> throw new IllegalStateException("No operation defined for [" + operation + "]");
        };
    }

}
