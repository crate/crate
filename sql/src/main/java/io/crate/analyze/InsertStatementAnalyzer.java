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

package io.crate.analyze;

import com.google.common.base.Preconditions;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.ValueSymbol;
import io.crate.sql.tree.*;
import org.cratedb.DataType;
import org.cratedb.sql.CrateException;

import java.util.ArrayList;
import java.util.List;

public class InsertStatementAnalyzer extends StatementAnalyzer<InsertAnalysis> {

    @Override
    public Symbol visitInsert(Insert node, InsertAnalysis context) {
        context.insertStatement(node);
        process(node.table(), context);

        int maxValuesLength = node.maxValuesLength();
        if (node.columns().size() == 0) {
            // no columns given in statement
            // num columns == max valuesList size
            Preconditions.checkState(maxValuesLength <= context.table().columns().size(), "too many values");
            List<Reference> impliedColumns = new ArrayList<>(maxValuesLength);
            int i = maxValuesLength;
            for (ReferenceInfo columnInfo : context.table().columns()) {
                if (i==0) { break; }
                impliedColumns.add(new Reference(columnInfo));
                i--;
            }
            context.columns(impliedColumns);

        } else {
            Preconditions.checkState(maxValuesLength == node.columns().size(), "invalid number of values");
            context.columns(new ArrayList<Reference>(node.columns().size()));
        }

        context.values(new ArrayList<List<Symbol>>(node.valuesLists().size()));

        for (QualifiedNameReference column : node.columns()) {
            process(column, context);
        }

        context.visitValues();
        for (ValuesList valuesList : node.valuesLists()) {
            process(valuesList, context);
        }

        return null;
    }

    @Override
    protected Symbol visitTable(Table node, InsertAnalysis context) {
        return super.visitTable(node, context);
    }

    /**
     * visit columns, if given in statement
     */
    @Override
    protected Symbol visitQualifiedNameReference(QualifiedNameReference node, InsertAnalysis context) {
        if (context.isVisitingValues()) {
            // column references not allowed in values, throw an error here
            throw new CrateException("column references not allowed in insert values.");
        }
        ReferenceIdent ident = new ReferenceIdent(context.table().ident(), node.getSuffix().getSuffix());
        // ensure that every column is only listed once
        Reference columnReference = context.allocateUniqueReference(ident);
        context.columns().add(columnReference);
        return columnReference;
    }

    @Override
    public Symbol visitValuesList(ValuesList node, InsertAnalysis context) {
        List<Symbol> symbols = new ArrayList<>();

        int i = 0;
        for (Expression value : node.values()) {
            Symbol valuesSymbol = process(value, context);
            assert valuesSymbol instanceof ValueSymbol;

            // implicit type conversion
            boolean wrongType = false;
            DataType expectedType = context.columns().get(i).valueType();
            if (valuesSymbol instanceof Literal) {
                try {
                    valuesSymbol = ((Literal) valuesSymbol).convertTo(expectedType);
                } catch (Exception e) {  // UnsupportedOperationException, NumberFormatException ...
                    wrongType = true;

                }
            } else if (((ValueSymbol)valuesSymbol).valueType() != expectedType) {
                wrongType = true;
            }

            if (wrongType) {
                throw new CrateException(String.format("Invalid type '%s' at value idx %d. expected '%s'",
                        ((ValueSymbol)valuesSymbol).valueType().getName(),
                        i,
                        expectedType.getName()));
            }

            symbols.add(valuesSymbol);
            i++;
        }
        context.values().add(symbols);
        return null;
    }
}
