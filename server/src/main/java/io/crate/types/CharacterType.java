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

package io.crate.types;

import static io.crate.common.StringUtils.isBlank;
import static io.crate.common.StringUtils.padEnd;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;

import io.crate.execution.dml.FulltextIndexer;
import io.crate.execution.dml.StringIndexer;
import io.crate.execution.dml.ValueIndexer;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.settings.SessionSettings;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.Expression;

public class CharacterType extends StringType {

    public static final String NAME = "character";
    public static final int ID = 27;
    public static final CharacterType INSTANCE = new CharacterType();

    private StorageSupport<Object> storageSupport(int lengthLimit) {
        return new StorageSupport<>(
            true,
            true,
            new StringEqQuery(value -> {
                if (value == null) {
                    return null;
                }
                // pads for values shorter than the length limit and also strips excess blank padding which should be ignored
                // when compared.
                if (value instanceof String s) {
                    return stripTrailingBlankPadding(padEnd(s, lengthLimit, ' '));
                }
                return stripTrailingBlankPadding(padEnd(((BytesRef) value).utf8ToString(), lengthLimit, ' '));
            })
        ) {
            @Override
            @SuppressWarnings({"rawtypes"})
            public ValueIndexer<Object> valueIndexer(RelationName table,
                                                     Reference ref,
                                                     Function<ColumnIdent, Reference> getRef) {
                return switch (ref.indexType()) {
                    case FULLTEXT -> (ValueIndexer) new FulltextIndexer(ref);
                    case NONE, PLAIN -> (ValueIndexer) new StringIndexer(ref);
                };
            }
        };
    }

    public static CharacterType of(List<Integer> parameters) {
        if (parameters.size() != 1) {
            throw new IllegalArgumentException(
                "The character type can only have a single parameter value, received: " +
                    parameters.size()
            );
        }
        return CharacterType.of(parameters.get(0));
    }

    public static CharacterType of(int lengthLimit) {
        if (lengthLimit <= 0) {
            throw new IllegalArgumentException(
                "The character type length must be at least 1, received: " + lengthLimit);
        }
        return new CharacterType(lengthLimit);
    }

    private final StorageSupport<Object> storageSupport;

    public CharacterType(StreamInput in) throws IOException {
        this(in.readInt());
    }

    private CharacterType(int lengthLimit) {
        super(lengthLimit);
        storageSupport = storageSupport(lengthLimit);
    }

    private CharacterType() {
        this(1);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public boolean unbound() {
        return false;
    }

    @Override
    public String valueForInsert(String value) {
        if (value == null) {
            return null;
        }
        if (value.length() == lengthLimit) {
            return value;
        } else if (value.length() < lengthLimit) {
            return padEnd(value, lengthLimit, ' ');
        } else {
            if (isBlank(value, lengthLimit, value.length())) {
                return value.substring(0, lengthLimit);
            } else {
                if (value.length() > 20) {
                    value = value.substring(0, 20) + "...";
                }
                throw new IllegalArgumentException(
                    "'" + value + "' is too long for the character type of length: " + lengthLimit);
            }
        }
    }

    @Override
    public String implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        var s = cast(value);
        if (s != null) {
            return padEnd(s, lengthLimit, ' ');
        }
        return s;
    }

    @Override
    public String explicitCast(Object value, SessionSettings sessionSettings) throws IllegalArgumentException, ClassCastException {
        if (value == null) {
            return null;
        }
        var string = cast(value);
        if (string.length() <= lengthLimit) {
            return padEnd(string, lengthLimit, ' ');
        } else {
            return string.substring(0, lengthLimit);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(lengthLimit);
    }

    @Override
    public ColumnType<Expression> toColumnType(@Nullable Supplier<List<ColumnDefinition<Expression>>> convertChildColumn) {
        return new ColumnType<>(NAME, List.of(lengthLimit));
    }

    @Override
    public Integer characterMaximumLength() {
        return lengthLimit;
    }

    @Override
    public TypeSignature getTypeSignature() {
        if (lengthLimit == 1) {
            return new TypeSignature(NAME);
        }
        return new TypeSignature(NAME, List.of(TypeSignature.of(lengthLimit)));
    }

    @Override
    public List<DataType<?>> getTypeParameters() {
        return List.of(DataTypes.INTEGER);
    }

    @Override
    public Precedence precedence() {
        return Precedence.CHARACTER;
    }

    @Override
    public void addMappingOptions(Map<String, Object> mapping) {
        mapping.put("length_limit", lengthLimit);
        mapping.put("blank_padding", true);
    }

    @Override
    public int compare(String val1, String val2) {
        // pads for values shorter than the length limit and also strips excess blank padding which should be ignored
        // when compared.
        return stripTrailingBlankPadding(padEnd(val1, lengthLimit, ' '))
            .compareTo(stripTrailingBlankPadding(padEnd(val2, lengthLimit, ' ')));
    }

    @Override
    public StorageSupport<Object> storageSupport() {
        return storageSupport;
    }

    private String stripTrailingBlankPadding(String s) {
        int idx = s.length();
        for (int i = idx - 1; i >= lengthLimit; i--) {
            if (Character.isSpaceChar(s.charAt(i))) {
                idx = i;
            } else {
                break;
            }
        }
        return s.substring(0, idx);
    }
}
