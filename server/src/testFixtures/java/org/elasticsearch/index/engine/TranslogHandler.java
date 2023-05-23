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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.RootObjectMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.mapper.MapperRegistry;

import io.crate.Constants;

public class TranslogHandler implements Engine.TranslogRecoveryRunner {

    private final MapperService mapperService;

    public TranslogHandler(NamedXContentRegistry xContentRegistry, IndexSettings indexSettings) {
        NamedAnalyzer defaultAnalyzer = new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer());
        IndexAnalyzers indexAnalyzers =
                new IndexAnalyzers(indexSettings, defaultAnalyzer, defaultAnalyzer, defaultAnalyzer, emptyMap(), emptyMap(), emptyMap());
        MapperRegistry mapperRegistry = new IndicesModule(emptyList()).getMapperRegistry();
        mapperService = new MapperService(indexSettings, indexAnalyzers, xContentRegistry, mapperRegistry);
    }

    private DocumentMapper docMapper(String type) {
        RootObjectMapper.Builder rootBuilder = new RootObjectMapper.Builder(type);
        DocumentMapper.Builder b = new DocumentMapper.Builder(rootBuilder, mapperService);
        return b.build(mapperService);
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
            applyOperation(engine, convertToEngineOp(operation, Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY));
            opsRecovered++;
        }
        engine.syncTranslog();
        return opsRecovered;
    }

    private Engine.Operation convertToEngineOp(Translog.Operation operation, Engine.Operation.Origin origin) {
        switch (operation.opType()) {
            case INDEX:
                final Translog.Index index = (Translog.Index) operation;
                final String indexName = mapperService.index().getName();
                return IndexShard.prepareIndex(
                    docMapper(Constants.DEFAULT_MAPPING_TYPE),
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
                    origin,
                    index.getAutoGeneratedIdTimestamp(),
                    true,
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    0
                );
            case DELETE:
                final Translog.Delete delete = (Translog.Delete) operation;
                return new Engine.Delete(
                    delete.id(),
                    delete.uid(),
                    delete.seqNo(),
                    delete.primaryTerm(),
                    delete.version(),
                    null,
                    origin,
                    System.nanoTime(),
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    0
                );
            case NO_OP:
                final Translog.NoOp noOp = (Translog.NoOp) operation;
                return new Engine.NoOp(noOp.seqNo(), noOp.primaryTerm(), origin, System.nanoTime(), noOp.reason());
            default:
                throw new IllegalStateException("No operation defined for [" + operation + "]");
        }
    }

}
