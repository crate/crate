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

package io.crate.operation.predicate;

import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;

import javax.annotation.Nullable;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Arrays;
import java.util.Locale;

/**
 * The match predicate is only used to generate lucene queries from.
  */
public class MatchPredicate implements FunctionImplementation<Function> {

    public static final String DEFAULT_MATCH_TYPE_STRING = MultiMatchQueryBuilder.Type.BEST_FIELDS.toString().toLowerCase(Locale.ENGLISH);
    public static final BytesRef DEFAULT_MATCH_TYPE = new BytesRef(DEFAULT_MATCH_TYPE_STRING);
    private static final DecimalFormat BOOST_FORMAT = new DecimalFormat("#.###", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
    public static final String NAME = "match";
    public static final FunctionIdent IDENT = new FunctionIdent(
            NAME,
            Arrays.<DataType>asList(DataTypes.OBJECT, DataTypes.STRING, DataTypes.STRING, DataTypes.OBJECT)
    );

    public static final FunctionInfo INFO = new FunctionInfo(IDENT, DataTypes.BOOLEAN, FunctionInfo.Type.PREDICATE);

    public static String fieldNameWithBoost(String fieldName, @Nullable Object boost) {
        if (boost == null) {
            return fieldName;
        }
        return String.format("%s^%s", fieldName, BOOST_FORMAT.format(boost));
    }

    /**
     * the match predicate is registered as a regular function
     * though it is called differently by the parser. We mangle
     * the arguments and options for the match predicate into
     * function arguments.
     *
     * 1. list of column idents and boost values - object mapping column name to boost value (Double) (values nullable)
     * 2. query string - string
     * 3. match_type - string (nullable)
     * 4. match_type options - object mapping option name to value (Object) (nullable)
     */
    public static void register(PredicateModule module) {
        module.register(new MatchPredicate());
    }

    public MatchPredicate() {
    }

    @Override
    public FunctionInfo info() {
        return INFO;
    }

    @Override
    public Symbol normalizeSymbol(Function function) {
        return function;
    }
}
