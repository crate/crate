/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.testing;

import com.google.common.collect.Lists;
import io.crate.DataType;
import io.crate.metadata.*;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.ValueSymbol;
import org.apache.lucene.util.BytesRef;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class TestingHelpers {

    /**
     * prints the contents of a result array as a human readable table
     * @param result the data to be printed
     * @return a string representing a table
     */
    public static String printedTable(Object[][] result) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(os);
        for (Object[] row : result) {
            boolean first = true;
            for (Object o : row) {
                if (!first) {
                    out.print("| ");
                } else {
                    first = false;
                }
                if (o == null) {
                    out.print("NULL");
                } else if (o instanceof BytesRef) {
                    out.print(((BytesRef) o).utf8ToString());
                } else {
                    out.print(o.toString());
                }
            }
            out.println();
        }
        return os.toString();
    }



    public static Function createFunction(String functionName, DataType returnType, List<Symbol> arguments) {
        List<DataType> dataTypes = Lists.transform(arguments, new com.google.common.base.Function<Symbol, DataType>() {
            @Nullable
            @Override
            public DataType apply(@Nullable Symbol input) {
                assert input instanceof ValueSymbol;
                return ((ValueSymbol) input).valueType();
            }
        });
        return new Function(
                new FunctionInfo(new FunctionIdent(functionName, dataTypes), returnType), arguments);
    }

    public static Reference createReference(String columnName, DataType dataType) {
        return createReference("dummyTable", new ColumnIdent(columnName), dataType);
    }

    public static Reference createReference(ColumnIdent columnIdent, DataType dataType) {
        return createReference("dummyTable", columnIdent, dataType);
    }

    public static Reference createReference(String tableName, ColumnIdent columnIdent, DataType dataType) {
        return new Reference(new ReferenceInfo(
                new ReferenceIdent(new TableIdent(null, tableName), columnIdent),
                RowGranularity.DOC,
                dataType
        ));
    }

    public static String readFile(String path) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new BytesRef(encoded).utf8ToString();
    }

}
