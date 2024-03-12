/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.statistics;

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;

import io.crate.types.DataType;

/**
 * Provides support for generating column statistics for a specific data type
 * @param <T>
 */
public interface ColumnStatsSupport<T> {

    /**
     * Creates a column sketch builder for a column
     */
    ColumnSketchBuilder<T> sketchBuilder();

    /**
     * Reads a column sketch from a stream
     */
    ColumnSketchBuilder<T> readSketchFrom(StreamInput in) throws IOException;

    /**
     * Creates a support instance for a single-valued datatype
     */
    static <T> ColumnStatsSupport<T> singleValued(Class<T> clazz, DataType<T> dataType) {
        return new ColumnStatsSupport<T>() {
            @Override
            public ColumnSketchBuilder<T> sketchBuilder() {
                return new ColumnSketchBuilder.SingleValued<>(clazz, dataType);
            }

            @Override
            public ColumnSketchBuilder<T> readSketchFrom(StreamInput in) throws IOException {
                return new ColumnSketchBuilder.SingleValued<>(clazz, dataType, in);
            }
        };
    }

    /**
     * Creates a support instance for a composite data type, e.g. an array
     */
    static <T> ColumnStatsSupport<T> composite(DataType<T> dataType) {
        return new ColumnStatsSupport<T>() {
            @Override
            public ColumnSketchBuilder<T> sketchBuilder() {
                return new ColumnSketchBuilder.Composite<>(dataType);
            }

            @Override
            public ColumnSketchBuilder<T> readSketchFrom(StreamInput in) throws IOException {
                return new ColumnSketchBuilder.Composite<>(dataType, in);
            }
        };
    }

}
