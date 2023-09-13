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

package io.crate.types;

import static io.crate.common.StringUtils.isBlank;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import org.jetbrains.annotations.Nullable;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import io.crate.Streamer;
import io.crate.common.unit.TimeValue;
import io.crate.execution.dml.FulltextIndexer;
import io.crate.execution.dml.StringIndexer;
import io.crate.execution.dml.ValueIndexer;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.settings.SessionSettings;
import io.crate.sql.tree.BitString;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.Expression;

public class StringType extends DataType<String> implements Streamer<String> {

    public static final int ID = 4;
    public static final StringType INSTANCE = new StringType();
    public static final String T = "t";
    public static final String F = "f";

    private static final StorageSupport<Object> STORAGE = new StorageSupport<>(
        true,
        true,
        true,
        new EqQuery<Object>() {

            @Override
            public Query termQuery(String field, Object value) {
                return new TermQuery(new Term(field, BytesRefs.toBytesRef(value)));
            }

            @Override
            public Query rangeQuery(String field,
                                    Object lowerTerm,
                                    Object upperTerm,
                                    boolean includeLower,
                                    boolean includeUpper,
                                    boolean hasDocValues) {
                return new TermRangeQuery(
                    field,
                    BytesRefs.toBytesRef(lowerTerm),
                    BytesRefs.toBytesRef(upperTerm),
                    includeLower,
                    includeUpper
                );
            }
        }
    ) {

        @Override
        @SuppressWarnings({"rawtypes", "unchecked"})
        public ValueIndexer<Object> valueIndexer(RelationName table,
                                                 Reference ref,
                                                 Function<String, FieldType> getFieldType,
                                                 Function<ColumnIdent, Reference> getRef) {
            FieldType fieldType = getFieldType.apply(ref.storageIdent());
            if (fieldType == null) {
                return (ValueIndexer) new StringIndexer(ref, fieldType);
            }
            return switch (ref.indexType()) {
                case FULLTEXT -> (ValueIndexer) new FulltextIndexer(ref, fieldType);
                case NONE, PLAIN -> (ValueIndexer) new StringIndexer(ref, fieldType);
            };
        }
    };

    private final int lengthLimit;

    public static StringType of(List<Integer> parameters) {
        if (parameters.size() != 1) {
            throw new IllegalArgumentException(
                "The text type can only have a single parameter value, received: " +
                parameters.size()
            );
        }
        return StringType.of(parameters.get(0));
    }

    public static StringType of(int lengthLimit) {
        if (lengthLimit <= 0) {
            throw new IllegalArgumentException(
                "The text type length must be at least 1, received: " + lengthLimit);
        }
        return new StringType(lengthLimit);
    }

    private StringType(int lengthLimit) {
        this.lengthLimit = lengthLimit;
    }

    public StringType(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_4_2_0)) {
            lengthLimit = in.readInt();
        } else {
            lengthLimit = Integer.MAX_VALUE;
        }
    }

    protected StringType() {
        this(Integer.MAX_VALUE);
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.STRING;
    }

    @Override
    public String getName() {
        return "text";
    }

    public int lengthLimit() {
        return lengthLimit;
    }

    public boolean unbound() {
        return lengthLimit == Integer.MAX_VALUE;
    }

    @Override
    public TypeSignature getTypeSignature() {
        if (unbound()) {
            return super.getTypeSignature();
        } else {
            return new TypeSignature(
                getName(),
                List.of(TypeSignature.of(lengthLimit())));
        }
    }

    @Override
    public List<DataType<?>> getTypeParameters() {
        if (unbound()) {
            return List.of();
        } else {
            return List.of(DataTypes.INTEGER);
        }
    }

    @Override
    public Streamer<String> streamer() {
        return this;
    }

    @Override
    public String implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        return cast(value);
    }

    protected String cast(Object value) throws IllegalArgumentException, ClassCastException {
        if (value == null) {
            return null;
        } else if (value instanceof String str) {
            return str;
        } else if (value instanceof BytesRef bytesRef) {
            return bytesRef.utf8ToString();
        } else if (value instanceof Boolean) {
            return (boolean) value ? T : F;
        } else if (value instanceof Map) {
            try {
                //noinspection unchecked
                return Strings.toString(JsonXContent.builder().map((Map<String, ?>) value));
            } catch (IOException e) {
                throw new IllegalArgumentException("Cannot cast `" + value + "` to type " + getName().toUpperCase(Locale.ENGLISH), e);
            }
        } else if (value instanceof Collection) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Cannot cast %s to type %s", value, getName().toUpperCase(Locale.ENGLISH)));
        } else if (value.getClass().isArray()) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Cannot cast %s to type %s",
                    Arrays.toString((Object[]) value),
                    getName().toUpperCase(Locale.ENGLISH)
                )
            );
        } else if (value instanceof TimeValue) {
            return ((TimeValue) value).getStringRep();
        } else if (value instanceof Regproc) {
            return ((Regproc) value).name();
        } else if (value instanceof Regclass) {
            return ((Regclass) value).name();
        } else if (value instanceof BitString bitString) {
            return bitString.asPrefixedBitString();
        } else {
            return value.toString();
        }
    }

    @Override
    public String explicitCast(Object value, SessionSettings sessionSettings) throws IllegalArgumentException, ClassCastException {
        if (value == null) {
            return null;
        }
        var string = cast(value);
        if (unbound() || string.length() <= lengthLimit()) {
            return string;
        } else {
            return string.substring(0, lengthLimit());
        }
    }

    @Override
    public String valueForInsert(Object value) {
        if (value == null) {
            return null;
        }
        assert value instanceof String
            : "valueForInsert must be called only on objects of String type";
        var string = (String) value;
        if (unbound() || string.length() <= lengthLimit) {
            return string;
        } else {
            if (isBlank(string, lengthLimit, string.length())) {
                return string.substring(0, lengthLimit);
            } else {
                if (string.length() > 20) {
                    string = string.substring(0, 20) + "...";
                }
                throw new IllegalArgumentException(
                    "'" + string + "' is too long for the text type of length: " + lengthLimit);
            }
        }
    }

    @Override
    public String sanitizeValue(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof BytesRef) {
            return ((BytesRef) value).utf8ToString();
        } else {
            return (String) value;
        }
    }

    @Override
    public boolean isConvertableTo(DataType<?> other, boolean explicitCast) {
        if (explicitCast) {
            if (other instanceof ArrayType<?> arrayType) {
                if (arrayType.innerType().id() == ID) {
                    return true;
                }
                if (arrayType.innerType().id() == JsonType.ID) {
                    return true;
                }
            } else if (other.id() == IntervalType.ID) {
                return true;
            }
        }
        return super.isConvertableTo(other, explicitCast);
    }

    @Override
    public int compare(String val1, String val2) {
        return val1.compareTo(val2);
    }

    @Override
    public String readValueFrom(StreamInput in) throws IOException {
        return in.readOptionalString();
    }

    @Override
    public void writeValueTo(StreamOutput out, String v) throws IOException {
        out.writeOptionalString(v);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_4_2_0)) {
            out.writeInt(lengthLimit);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        StringType that = (StringType) o;
        return lengthLimit() == that.lengthLimit();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), lengthLimit());
    }

    @Override
    public ColumnType<Expression> toColumnType(ColumnPolicy columnPolicy,
                                               @Nullable Supplier<List<ColumnDefinition<Expression>>> convertChildColumn) {
        if (unbound()) {
            return new ColumnType<>(getName());
        } else {
            return new ColumnType<>("varchar", List.of(lengthLimit));
        }
    }

    @Override
    public StorageSupport<Object> storageSupport() {
        return STORAGE;
    }

    @Override
    public Integer characterMaximumLength() {
        if (unbound()) {
            return null;
        } else {
            return lengthLimit();
        }
    }

    @Override
    public long valueBytes(String value) {
        return RamUsageEstimator.sizeOf(value);
    }

    @Override
    public void addMappingOptions(Map<String, Object> mapping) {
        if (!unbound()) {
            mapping.put("length_limit", lengthLimit);
        }
    }
}
