/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.lucene.search.function;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * A function_score function that multiplies the score with the value of a
 * field from the document, optionally multiplying the field by a factor first,
 * and applying a modification (log, ln, sqrt, square, etc) afterwards.
 */
public class FieldValueFactorFunction extends ScoreFunction {
    private final String field;
    private final float boostFactor;
    private final Modifier modifier;
    /**
     * Value used if the document is missing the field.
     */
    private final Double missing;
    private final IndexNumericFieldData indexFieldData;

    public FieldValueFactorFunction(String field, float boostFactor, Modifier modifierType, Double missing,
            IndexNumericFieldData indexFieldData) {
        super(CombineFunction.MULTIPLY);
        this.field = field;
        this.boostFactor = boostFactor;
        this.modifier = modifierType;
        this.indexFieldData = indexFieldData;
        this.missing = missing;
    }

    @Override
    public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) {
        final SortedNumericDoubleValues values;
        if(indexFieldData == null) {
            values = FieldData.emptySortedNumericDoubles();
        } else {
            values = this.indexFieldData.load(ctx).getDoubleValues();
        }

        return new LeafScoreFunction() {

            @Override
            public double score(int docId, float subQueryScore) throws IOException {
                double value;
                if (values.advanceExact(docId)) {
                    value = values.nextValue();
                } else {
                    if (missing != null) {
                        value = missing;
                    } else {
                        throw new ElasticsearchException("Missing value for field [" + field + "]");
                    }
                }
                double val = value * boostFactor;
                double result = modifier.apply(val);
                return result;
            }

            @Override
            public Explanation explainScore(int docId, Explanation subQueryScore) throws IOException {
                String modifierStr = modifier != null ? modifier.toString() : "";
                String defaultStr = missing != null ? "?:" + missing : "";
                double score = score(docId, subQueryScore.getValue());
                return Explanation.match(
                        (float) score,
                        String.format(Locale.ROOT,
                                "field value function: %s(doc['%s'].value%s * factor=%s)", modifierStr, field, defaultStr, boostFactor));
            }
        };
    }

    @Override
    public boolean needsScores() {
        return false;
    }

    @Override
    protected boolean doEquals(ScoreFunction other) {
        FieldValueFactorFunction fieldValueFactorFunction = (FieldValueFactorFunction) other;
        return this.boostFactor == fieldValueFactorFunction.boostFactor &&
                Objects.equals(this.field, fieldValueFactorFunction.field) &&
                Objects.equals(this.modifier, fieldValueFactorFunction.modifier);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(boostFactor, field, modifier);
    }

    /**
     * The Type class encapsulates the modification types that can be applied
     * to the score/value product.
     */
    public enum Modifier implements Writeable {
        NONE {
            @Override
            public double apply(double n) {
                return n;
            }
        },
        LOG {
            @Override
            public double apply(double n) {
                return Math.log10(n);
            }
        },
        LOG1P {
            @Override
            public double apply(double n) {
                return Math.log10(n + 1);
            }
        },
        LOG2P {
            @Override
            public double apply(double n) {
                return Math.log10(n + 2);
            }
        },
        LN {
            @Override
            public double apply(double n) {
                return Math.log(n);
            }
        },
        LN1P {
            @Override
            public double apply(double n) {
                return Math.log1p(n);
            }
        },
        LN2P {
            @Override
            public double apply(double n) {
                return Math.log1p(n + 1);
            }
        },
        SQUARE {
            @Override
            public double apply(double n) {
                return Math.pow(n, 2);
            }
        },
        SQRT {
            @Override
            public double apply(double n) {
                return Math.sqrt(n);
            }
        },
        RECIPROCAL {
            @Override
            public double apply(double n) {
                return 1.0 / n;
            }
        };

        public abstract double apply(double n);

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        public static Modifier readFromStream(StreamInput in) throws IOException {
            return in.readEnum(Modifier.class);
        }

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }

        public static Modifier fromString(String modifier) {
            return valueOf(modifier.toUpperCase(Locale.ROOT));
        }
    }
}
