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

package io.crate.expression.predicate;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.index.query.MultiMatchQueryType;

import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionName;
import io.crate.metadata.FunctionType;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

/**
 * The match predicate is only used to generate lucene queries from.
 */
public class MatchPredicate implements FunctionImplementation {

    public static final String NAME = "match";

    /**
     * the match predicate is registered as a regular function
     * though it is called differently by the parser. We mangle
     * the arguments and options for the match predicate into
     * function arguments.
     * <p>
     * 1. list of column idents and boost values - object mapping column name to boost value (Double) (values nullable)
     * 2. query string - string
     * 3. match_type - string (nullable)
     * 4. match_type options - object mapping option name to value (Object) (nullable)
     */
    public static final Signature TEXT_MATCH = Signature.scalar(
        NAME,
        DataTypes.UNTYPED_OBJECT.getTypeSignature(),
        DataTypes.STRING.getTypeSignature(),
        DataTypes.STRING.getTypeSignature(),
        DataTypes.UNTYPED_OBJECT.getTypeSignature(),
        DataTypes.BOOLEAN.getTypeSignature()
    );

    public static final Signature GEO_MATCH = Signature.scalar(
        NAME,
        DataTypes.UNTYPED_OBJECT.getTypeSignature(),
        DataTypes.GEO_SHAPE.getTypeSignature(),
        DataTypes.STRING.getTypeSignature(),
        DataTypes.UNTYPED_OBJECT.getTypeSignature(),
        DataTypes.BOOLEAN.getTypeSignature()
    );


    // IDENT / INFO here are for BWC of mixed clusters running 4.1 and 4.2;
    public static final FunctionIdent IDENT = new FunctionIdent(
        new FunctionName(null, NAME),
        Arrays.asList(ObjectType.UNTYPED, DataTypes.STRING, DataTypes.STRING, ObjectType.UNTYPED)
    );
    public static final FunctionInfo INFO = new FunctionInfo(IDENT, DataTypes.BOOLEAN, FunctionType.SCALAR, Set.of());

    public static void register(PredicateModule module) {
        module.register(
            GEO_MATCH,
            MatchPredicate::new
        );
        module.register(
            TEXT_MATCH,
            MatchPredicate::new
        );
    }

    public static final Set<DataType<?>> SUPPORTED_TYPES = Set.of(DataTypes.STRING, DataTypes.GEO_SHAPE);

    private static final Map<DataType<?>, String> DATA_TYPE_TO_DEFAULT_MATCH_TYPE = Map.of(
        DataTypes.STRING,
        MultiMatchQueryType.BEST_FIELDS.toString().toLowerCase(Locale.ENGLISH),
        DataTypes.GEO_SHAPE,
        "intersects"
    );

    private static final Set<String> SUPPORTED_GEO_MATCH_TYPES = Set.of("intersects", "disjoint", "within");

    private final Signature signature;
    private final Signature boundSignature;

    private MatchPredicate(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    private static String defaultMatchType(DataType<?> dataType) {
        String matchType = DATA_TYPE_TO_DEFAULT_MATCH_TYPE.get(dataType);
        if (matchType == null) {
            throw new IllegalArgumentException("No default matchType found for dataType: " + dataType);
        }
        return matchType;
    }

    public static String getMatchType(@Nullable String matchType, DataType<?> columnType) {
        if (matchType == null) {
            return defaultMatchType(columnType);
        }
        if (columnType.equals(DataTypes.STRING)) {
            try {
                MultiMatchQueryType.parse(matchType, LoggingDeprecationHandler.INSTANCE);
                return matchType;
            } catch (ElasticsearchParseException e) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "invalid MATCH type '%s' for type '%s'",
                    matchType,
                    columnType), e);
            }
        } else if (columnType.equals(DataTypes.GEO_SHAPE)) {
            if (!SUPPORTED_GEO_MATCH_TYPES.contains(matchType)) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "invalid MATCH type '%s' for type '%s', valid types are: [%s]",
                    matchType,
                    columnType,
                    String.join(",", SUPPORTED_GEO_MATCH_TYPES)));
            }
            return matchType;
        }
        throw new IllegalArgumentException("No match type for dataType: " + columnType);
    }
}
