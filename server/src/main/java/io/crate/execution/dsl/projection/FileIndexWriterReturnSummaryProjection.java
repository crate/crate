///*
// * Licensed to Crate.io GmbH ("Crate") under one or more contributor
// * license agreements.  See the NOTICE file distributed with this work for
// * additional information regarding copyright ownership.  Crate licenses
// * this file to you under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.  You may
// * obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// * License for the specific language governing permissions and limitations
// * under the License.
// *
// * However, if you have executed another commercial license agreement
// * with Crate these terms will supersede the license and you may use the
// * software solely pursuant to the terms of the relevant commercial agreement.
// */
//
//package io.crate.execution.dsl.projection;
//
//import io.crate.expression.symbol.InputColumn;
//import io.crate.expression.symbol.Symbol;
//import io.crate.metadata.ColumnIdent;
//import io.crate.metadata.Reference;
//import io.crate.metadata.RelationName;
//import org.elasticsearch.common.settings.Settings;
//import org.jetbrains.annotations.Nullable;
//
//import java.util.List;
//
//public class FileIndexWriterReturnSummaryProjection extends FileIndexWriterProjection {
//
//    public FileIndexWriterReturnSummaryProjection(RelationName relationName,
//                                                  @Nullable String partitionIdent,
//                                                  Reference rawSourceReference,
//                                                  InputColumn rawSourcePtr,
//                                                  List<Reference> partitionByReferences,
//                                                  List<ColumnIdent> primaryKeys,
//                                                 @Nullable ColumnIdent clusteredByColumn,
//                                                  Settings settings,
//                                                  List<? extends Symbol> outputs,
//                                                  boolean autoCreateIndices,
//                                                  InputColumn sourceUri,
//                                                  InputColumn sourceUriFailure,
//                                                  InputColumn lineNumber) {
//        super(relationName, partitionIdent, primaryKeys, clusteredByColumn, settings, autoCreateIndices);
//        this.rawSourceReference = rawSourceReference;
//        this.rawSourceSymbol = rawSourcePtr;
//        this.outputs = outputs;
//        overwriteDuplicates = settings.getAsBoolean(OVERWRITE_DUPLICATES, OVERWRITE_DUPLICATES_DEFAULT);
//        this.failFast = settings.getAsBoolean(FAIL_FAST, false);
//        this.validation = settings.getAsBoolean(SOURCE_VALIDATION, true);
//    }
//
////    @Override
////    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
////        return visitor.visitFileIndexWriterReturnSummaryProjection(this, context);
////    }
//
//    public InputColumn rawSource() {
//        return rawSourceSymbol;
//    }
//
//    public Reference rawSourceReference() {
//        return rawSourceReference;
//    }
//
//    @Override
//    public ProjectionType projectionType() {
//        return ProjectionType.FILE_WRITER_RETURN_SUMMARY;
//    }
//}
