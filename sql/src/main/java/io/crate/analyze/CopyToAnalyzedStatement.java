/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Path;
import io.crate.planner.projection.WriterProjection;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import javax.print.attribute.standard.Compression;
import java.util.List;
import java.util.Map;

public class CopyToAnalyzedStatement extends AbstractCopyAnalyzedStatement implements AnalyzedRelation {

    private final QueriedDocTable subQueryRelation;
    private final boolean columnsDefined;
    @Nullable
    private final WriterProjection.CompressionType compressionType;
    @Nullable
    private final WriterProjection.OutputFormat outputFormat;
    @Nullable
    private final List<String> outputNames;
    private final boolean isDirectoryUri;

    /*
     * add values that should be added or overwritten
     * all symbols must normalize to literals on the shard level.
     */
    private final Map<ColumnIdent, Symbol> overwrites;

    public CopyToAnalyzedStatement(QueriedDocTable subQueryRelation,
                                   Settings settings,
                                   Symbol uri,
                                   boolean isDirectoryUri,
                                   @Nullable WriterProjection.CompressionType compressionType,
                                   @Nullable WriterProjection.OutputFormat outputFormat,
                                   @Nullable List<String> outputNames,
                                   boolean columnsDefined,
                                   @Nullable Map<ColumnIdent, Symbol> overwrites) {
        super(settings, uri);
        this.subQueryRelation = subQueryRelation;
        this.columnsDefined = columnsDefined;
        this.compressionType = compressionType;
        this.outputNames = outputNames;
        this.outputFormat = outputFormat;
        this.isDirectoryUri = isDirectoryUri;
        this.overwrites = MoreObjects.firstNonNull(overwrites, ImmutableMap.<ColumnIdent, Symbol>of());
    }

    public QueriedDocTable subQueryRelation() {
        return subQueryRelation;
    }

    public boolean columnsDefined() {
        return columnsDefined;
    }

    @Nullable
    public WriterProjection.CompressionType compressionType() { return compressionType; }

    @Nullable
    public WriterProjection.OutputFormat outputFormat() { return outputFormat; }

    @Nullable
    public List<String> outputNames() { return outputNames; }

    public boolean isDirectoryUri() {
        return isDirectoryUri;
    }

    public Map<ColumnIdent, Symbol> overwrites() {
        return this.overwrites;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitCopyToStatement(this, context);
    }

    @Nullable
    @Override
    public Field getField(Path path) {
        throw new UnsupportedOperationException("getField not implemented on CopyToAnalyzedStatement");
    }

    @Nullable
    @Override
    public Field getWritableField(Path path) throws UnsupportedOperationException, ColumnUnknownException {
        throw new UnsupportedOperationException("CopyToAnalyzedStatement is not writable");
    }

    @Override
    public List<Field> fields() {
        return ImmutableList.of();
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitCopyToAnalyzedStatement(this, context);
    }
}
