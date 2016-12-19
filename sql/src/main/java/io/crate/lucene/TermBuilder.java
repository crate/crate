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

package io.crate.lucene;

import io.crate.types.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.mapper.BooleanFieldMapper;

import java.util.Locale;

abstract class TermBuilder<T> {

    private static final IntegerTermBuilder INTEGER_TERM_BUILDER = new IntegerTermBuilder();
    private static final ByteTermBuilder BYTE_TERM_BUILDER = new ByteTermBuilder();
    private static final ShortTermBuilder SHORT_TERM_BUILDER = new ShortTermBuilder();
    private static final LongTermBuilder LONG_TERM_BUILDER = new LongTermBuilder();
    private static final BooleanTermBuilder BOOLEAN_TERM_BUILDER = new BooleanTermBuilder();
    private static final DoubleTermBuilder DOUBLE_TERM_BUILDER = new DoubleTermBuilder();
    private static final FloatTermBuilder FLOAT_TERM_BUILDER = new FloatTermBuilder();
    private static final StringTermBuilder STRING_TERM_BUILDER = new StringTermBuilder();

    static TermBuilder forType(DataType dataType) {
        while (dataType instanceof CollectionType) {
            dataType = ((CollectionType) dataType).innerType();
        }

        switch (dataType.id()) {
            case BooleanType.ID:
                return BOOLEAN_TERM_BUILDER;
            case IntegerType.ID:
                return INTEGER_TERM_BUILDER;
            case ByteType.ID:
                return BYTE_TERM_BUILDER;
            case ShortType.ID:
                return SHORT_TERM_BUILDER;
            case LongType.ID:
            case TimestampType.ID:
            case IpType.ID:
                return LONG_TERM_BUILDER;
            case FloatType.ID:
                return FLOAT_TERM_BUILDER;
            case DoubleType.ID:
                return DOUBLE_TERM_BUILDER;
            case StringType.ID:
                return STRING_TERM_BUILDER;
            default:
                throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "type %s not supported", dataType));
        }
    }

    public abstract BytesRef term(T value);

    private static class IntegerTermBuilder extends TermBuilder<Integer> {
        @Override
        public BytesRef term(Integer value) {
            BytesRefBuilder builder = new BytesRefBuilder();
            NumericUtils.intToPrefixCoded(value, 0, builder);
            return builder.get();
        }
    }

    private static class ByteTermBuilder extends TermBuilder<Byte> {
        @Override
        public BytesRef term(Byte value) {
            return INTEGER_TERM_BUILDER.term(value.intValue());
        }
    }

    private static class ShortTermBuilder extends TermBuilder<Short> {
        @Override
        public BytesRef term(Short value) {
            return INTEGER_TERM_BUILDER.term(value.intValue());
        }
    }

    private static class LongTermBuilder extends TermBuilder<Long> {
        @Override
        public BytesRef term(Long value) {
            BytesRefBuilder builder = new BytesRefBuilder();
            NumericUtils.longToPrefixCoded(value, 0, builder);
            return builder.get();
        }
    }

    private static class DoubleTermBuilder extends TermBuilder<Double> {
        @Override
        public BytesRef term(Double value) {
            long longValue = NumericUtils.doubleToSortableLong(value);
            BytesRefBuilder bytesRef = new BytesRefBuilder();
            NumericUtils.longToPrefixCoded(longValue, 0, bytesRef);
            return bytesRef.get();
        }
    }

    private static class FloatTermBuilder extends TermBuilder<Float> {
        @Override
        public BytesRef term(Float value) {
            int intValue = NumericUtils.floatToSortableInt(value);
            BytesRefBuilder bytesRef = new BytesRefBuilder();
            NumericUtils.intToPrefixCoded(intValue, 0, bytesRef);
            return bytesRef.get();
        }
    }

    private static class BooleanTermBuilder extends TermBuilder<Boolean> {
        @Override
        public BytesRef term(Boolean value) {
            if (value == null) {
                return BooleanFieldMapper.Values.FALSE;
            }
            return value ? BooleanFieldMapper.Values.TRUE : BooleanFieldMapper.Values.FALSE;
        }
    }

    private static class StringTermBuilder extends TermBuilder<BytesRef> {
        @Override
        public BytesRef term(BytesRef value) {
            return value;
        }
    }
}
