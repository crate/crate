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

package io.crate.fdw;

import java.nio.file.Path;
import java.util.List;

import dev.hardwood.OutputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriter;
import io.crate.data.Row;
import io.crate.metadata.Reference;

public class ParquetWriter {
    private RowWriter rowWriter;
    private final List<Reference> columns;
    private ParquetFileWriter writer;

    public ParquetWriter(Path outputFilePath, List<Reference> columns) throws Exception {
        this.columns = columns;
        // TODO: handle adding columns dynamically
        // TODO: mapping from crate types to the parquet/hardwood types
        FileSchema schema = FileSchema.builder("schema")
                .addColumn(columns.get(0).toString(), PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                .addColumn(columns.get(1).toString(), PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                        new LogicalType.StringType())
                .build();

        if (writer == null) {
            writer = ParquetFileWriter.create(OutputFile.of(outputFilePath), schema);
            rowWriter = writer.rowWriter();
        }
    }

    public void writeRows(List<Row> rows) throws Exception {
        for (Row row : rows) {
            // TODO: handle typed setter and column names dynamically
            rowWriter.writeRow(r -> r
                    .setDouble(columns.get(0).toString(), (double) row.get(0))
                    .setString(columns.get(1).toString(), "hello"));
        }
    }

    public void close() throws Exception {
        if (writer != null) {
            writer.close();
            return;
        }
        System.out.println("Unexpected null writer");
    }

}
