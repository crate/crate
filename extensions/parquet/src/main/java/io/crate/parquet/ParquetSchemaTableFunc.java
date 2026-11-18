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

package io.crate.parquet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.settings.Settings;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.opendal.SharedAsyncExecutor;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.types.DataTypes;
import io.crate.types.RowType;

public class ParquetSchemaTableFunc extends TableFunctionImplementation<String> {

    private static final String NAME = "parquet_schema";
    private static final RowType RETURN_TYPE = new RowType(
        List.of(
            DataTypes.STRING,
            DataTypes.STRING,
            DataTypes.INTEGER,
            DataTypes.STRING,
            DataTypes.STRING,
            DataTypes.INTEGER
        ),
        List.of("name", "type", "type_length", "repetition_type", "logical_type", "column_index")
    );

    public static void register(Functions.Builder builder) {
        Signature signature = Signature.builder(NAME, FunctionType.TABLE)
            .argumentTypes(DataTypes.STRING.getTypeSignature())
            .returnType(RETURN_TYPE.getTypeSignature())
            .build();
        builder.add(signature, ParquetSchemaTableFunc::new);
    }

    private ParquetSchemaTableFunc(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    public RowType returnType() {
        return RETURN_TYPE;
    }

    @Override
    public boolean hasLazyResultSet() {
        return true;
    }

    @Override
    @SafeVarargs
    public final Iterable<Row> evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<String>... args) {
        String uri = args[0].value();
        if (uri == null) {
            throw new NullPointerException("URI argument to parquet_schema must not be null");
        }
        Roles roles = nodeContext.roles();
        Role user = roles.getUser(txnCtx.sessionSettings().userName());
        try (var sharedAsyncExecutor = SharedAsyncExecutor.getInstance(Settings.EMPTY)) {
            List<InputFile> inputFiles = ParquetForeignDataWrapper.urisToInputFiles(
                sharedAsyncExecutor.asyncExecutor(),
                user,
                List.of(uri),
                Settings.EMPTY
            );
            try (ParquetFileReader reader = ParquetFileReader.openAll(inputFiles)) {
                FileSchema fileSchema = reader.getFileSchema();
                List<ColumnSchema> columns = fileSchema.getColumns();
                ArrayList<Row> result = new ArrayList<>();
                for (var column : columns) {
                    String name = column.name();
                    PhysicalType type = column.type();
                    RepetitionType repetitionType = column.repetitionType();
                    LogicalType logicalType = column.logicalType();
                    RowN row = new RowN(
                        name,
                        type.toString(),
                        column.typeLength(),
                        repetitionType == null ? null : repetitionType.toString(),
                        logicalType == null ? null : logicalType.toString(),
                        column.columnIndex()
                    );
                    result.add(row);
                }
                return result;
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }
}
