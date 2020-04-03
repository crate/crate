/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import io.crate.Streamer;
import io.crate.common.collections.MapComparator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static io.crate.types.DataTypes.ALLOWED_CONVERSIONS;
import static io.crate.types.DataTypes.UNDEFINED;

public class ObjectType extends DataType<Map<String, Object>> implements Streamer<Map<String, Object>> {

    public static final int ID = 12;

    public static final String NAME = "object";

    public static ObjectType untyped() {
        return new ObjectType();
    }

    public static class Builder {

        Map<String, DataType<?>> innerTypesBuilder = new HashMap<>();

        public Builder setInnerType(String key, DataType<?> innerType) {
            innerTypesBuilder.put(key, innerType);
            return this;
        }

        public ObjectType build() {
            return new ObjectType(Collections.unmodifiableMap(innerTypesBuilder));
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private final Map<String, DataType<?>> innerTypes;

    private ObjectType() {
        this(Map.of());
    }

    private ObjectType(Map<String, DataType<?>> innerTypes) {
        this.innerTypes = innerTypes;
    }

    public Map<String, DataType<?>> innerTypes() {
        return innerTypes;
    }

    public DataType<?> innerType(String key) {
        return innerTypes.getOrDefault(key, UndefinedType.INSTANCE);
    }

    public DataType<?> resolveInnerType(List<String> path) {
        if (path.isEmpty()) {
            return DataTypes.UNDEFINED;
        }
        DataType<?> innerType = DataTypes.UNDEFINED;
        ObjectType currentObject = this;
        for (int i = 0; i < path.size(); i++) {
            innerType = currentObject.innerTypes().get(path.get(i));
            if (innerType == null) {
                return DataTypes.UNDEFINED;
            }
            if (innerType.id() == ID) {
                currentObject = (ObjectType) innerType;
            }
        }
        return innerType;
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.OBJECT;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Streamer<Map<String, Object>> streamer() {
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> value(Object value) {
        if (value instanceof String) {
            value = mapFromJSONString((String) value);
        }
        Map<String, Object> map = (Map<String, Object>) value;

        if (map == null || innerTypes == null) {
            return map;
        }

        HashMap<String, Object> newMap = new HashMap<>(map);
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            DataType innerType = innerTypes.getOrDefault(key, UndefinedType.INSTANCE);
            newMap.put(key, innerType.value(entry.getValue()));
        }
        return newMap;
    }

    private static Map<String,Object> mapFromJSONString(String value) {
        try {
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                value
            );
            return parser.map();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int compare(Map<String, Object> val1, Map<String, Object> val2) {
        return MapComparator.compareMaps(val1, val2);
    }

    @Override
    public Map<String, Object> readValueFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            int size = in.readInt();
            HashMap<String, Object> m = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                String key = in.readString();
                DataType innerType = innerTypes.getOrDefault(key, UndefinedType.INSTANCE);
                Object val = innerType.streamer().readValueFrom(in);
                m.put(key, val);
            }
            return m;
        }
        return null;
    }

    @Override
    public boolean isConvertableTo(DataType<?> o) {
        Set<Integer> conversions = ALLOWED_CONVERSIONS.getOrDefault(id(), Set.of());
        if (conversions.contains(o.id())) {
            return true;
        }

        if (o.id() != id() || o.id() == UNDEFINED.id()) {
            return false;
        }
        ObjectType that = (ObjectType) o;
        if (innerTypes.isEmpty() || that.innerTypes().isEmpty()) {
            return true;
        } else {
            for (var thisInnerField : this.innerTypes.entrySet()) {
                var thisInnerType = thisInnerField.getValue();
                var thatInnerType = that.innerTypes().get(thisInnerField.getKey());
                if (thatInnerType == null
                    || !thisInnerType.isConvertableTo(thatInnerType)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        ObjectType that = (ObjectType) o;
        return Objects.equals(innerTypes, that.innerTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), innerTypes);
    }

    @Override
    public void writeValueTo(StreamOutput out, Map<String, Object> v) throws IOException {
        if (v == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeInt(v.size());
            for (Map.Entry<String, Object> entry : v.entrySet()) {
                String key = entry.getKey();
                out.writeString(key);
                DataType innerType = innerTypes.getOrDefault(key, UndefinedType.INSTANCE);
                //noinspection unchecked
                innerType.streamer().writeValueTo(out, innerType.value(entry.getValue()));
            }
        }
    }

    public ObjectType(StreamInput in) throws IOException {
        int typesSize = in.readVInt();
        Map<String, DataType<?>> builder = new HashMap<>();
        for (int i = 0; i < typesSize; i++) {
            String key = in.readString();
            DataType<?> type = DataTypes.fromStream(in);
            builder.put(key, type);
        }
        innerTypes = Collections.unmodifiableMap(builder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(innerTypes.size());
        for (Map.Entry<String, DataType<?>> entry : innerTypes.entrySet()) {
            out.writeString(entry.getKey());
            DataTypes.toStream(entry.getValue(), out);
        }
    }

    @Override
    public TypeSignature getTypeSignature() {
        ArrayList<TypeSignature> parameters = new ArrayList<>(innerTypes.size() * 2);
        for (var innerTypeKeyValue : innerTypes.entrySet()) {
            // all keys are of type 'text'
            parameters.add(StringType.INSTANCE.getTypeSignature());

            var innerType = innerTypeKeyValue.getValue();
            parameters.add(
                new ObjectParameterTypeSignature(innerTypeKeyValue.getKey(), innerType.getTypeSignature()));
        }
        return new TypeSignature(NAME, parameters);
    }

    @Override
    public List<DataType<?>> getTypeParameters() {
        ArrayList<DataType<?>> parameters = new ArrayList<>(innerTypes.size() * 2);
        for (var type : innerTypes.values()) {
            // all keys are of type 'text'
            parameters.add(StringType.INSTANCE);
            parameters.add(type);
        }
        return parameters;
    }
}
