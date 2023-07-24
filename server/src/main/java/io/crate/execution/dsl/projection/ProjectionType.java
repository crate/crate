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

package io.crate.execution.dsl.projection;

import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public enum ProjectionType {

    LIMITANDOFFSET(LimitAndOffsetProjection::new),
    GROUP(GroupProjection::new),
    AGGREGATION(AggregationProjection::new),
    MERGE_COUNT_AGGREGATION(i -> MergeCountProjection.INSTANCE),
    FILTER(FilterProjection::new),
    WRITER(WriterProjection::new),
    INDEX_WRITER(SourceIndexWriterProjection::new),
    INDEX_WRITER_RETURN_SUMMARY(SourceIndexWriterReturnSummaryProjection::new),
    COLUMN_INDEX_WRITER(ColumnIndexWriterProjection::new),
    UPDATE(UpdateProjection::new),
    SYS_UPDATE(SysUpdateProjection::new),
    DELETE(DeleteProjection::new),
    FETCH(null),
    LIMITANDOFFSET_ORDERED(OrderedLimitAndOffsetProjection::new),
    EVAL(EvalProjection::new),
    PROJECT_SET(ProjectSetProjection::new),
    WINDOW_AGGREGATION(WindowAggProjection::new),
    LIMIT_DISTINCT(LimitDistinctProjection::new),
    CORRELATED_JOIN(in -> {
        throw new UnsupportedOperationException("Cannot stream correlated join projection");
    }),
    FILE_WRITER(FileIndexWriterProjection::new),
    FILE_WRITER_RETURN_SUMMARY(FileIndexWriterReturnSummaryProjection::new);

    private final Projection.ProjectionFactory<?> factory;

    ProjectionType(Projection.ProjectionFactory<?> factory) {
        this.factory = factory;
    }

    public Projection newInstance(StreamInput in) throws IOException {
        return factory.newInstance(in);
    }
}
