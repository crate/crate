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

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * This abstract class defining basic {@link Decision} used during shard
 * allocation process.
 *
 * @see AllocationDecider
 */
public abstract class Decision implements ToXContent, Writeable {

    public static final Decision ALWAYS = new Single(Type.YES);
    public static final Decision YES = new Single(Type.YES);
    public static final Decision NO = new Single(Type.NO);
    public static final Decision THROTTLE = new Single(Type.THROTTLE);

    /**
     * Creates a simple decision
     * @param type {@link Type} of the decision
     * @param label label for the Decider that produced this decision
     * @param explanation explanation of the decision
     * @param explanationParams additional parameters for the decision
     * @return new {@link Decision} instance
     */
    public static Decision single(Type type, @Nullable String label, @Nullable String explanation, @Nullable Object... explanationParams) {
        return new Single(type, label, explanation, explanationParams);
    }

    public static Decision readFrom(StreamInput in) throws IOException {
        // Determine whether to read a Single or Multi Decision
        if (in.readBoolean()) {
            Multi result = new Multi();
            int decisionCount = in.readVInt();
            for (int i = 0; i < decisionCount; i++) {
                Decision s = readFrom(in);
                result.decisions.add(s);
            }
            return result;
        } else {
            Single result = new Single();
            result.type = Type.readFrom(in);
            result.label = in.readOptionalString();
            result.explanationString = in.readOptionalString();
            return result;
        }
    }

    /**
     * This enumeration defines the
     * possible types of decisions
     */
    public enum Type implements Writeable {
        YES(1),
        THROTTLE(2),
        NO(0);

        private final int id;

        Type(int id) {
            this.id = id;
        }

        public static Type resolve(String s) {
            return Type.valueOf(s.toUpperCase(Locale.ROOT));
        }

        public static Type readFrom(StreamInput in) throws IOException {
            int i = in.readVInt();
            switch (i) {
                case 0:
                    return NO;
                case 1:
                    return YES;
                case 2:
                    return THROTTLE;
                default:
                    throw new IllegalArgumentException("No Type for integer [" + i + "]");
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(id);
        }

        public boolean higherThan(Type other) {
            if (this == NO) {
                return false;
            } else if (other == NO) {
                return true;
            } else if (other == THROTTLE && this == YES) {
                return true;
            }
            return false;
        }

    }

    /**
     * Get the {@link Type} of this decision
     * @return {@link Type} of this decision
     */
    public abstract Type type();

    /**
     * Get the description label for this decision.
     */
    @Nullable
    public abstract String label();

    /**
     * Get the explanation for this decision.
     */
    @Nullable
    public abstract String getExplanation();

    /**
     * Return the list of all decisions that make up this decision
     */
    public abstract List<Decision> getDecisions();

    /**
     * Simple class representing a single decision
     */
    public static class Single extends Decision {
        private Type type;
        private String label;
        private String explanation;
        private String explanationString;
        private Object[] explanationParams;

        public Single() {

        }

        /**
         * Creates a new {@link Single} decision of a given type
         * @param type {@link Type} of the decision
         */
        public Single(Type type) {
            this(type, null, null, (Object[]) null);
        }

        /**
         * Creates a new {@link Single} decision of a given type
         *
         * @param type {@link Type} of the decision
         * @param explanation An explanation of this {@link Decision}
         * @param explanationParams A set of additional parameters
         */
        public Single(Type type, @Nullable String label, @Nullable String explanation, @Nullable Object... explanationParams) {
            this.type = type;
            this.label = label;
            this.explanation = explanation;
            this.explanationParams = explanationParams;
        }

        @Override
        public Type type() {
            return this.type;
        }

        @Override
        @Nullable
        public String label() {
            return this.label;
        }

        @Override
        public List<Decision> getDecisions() {
            return Collections.singletonList(this);
        }

        /**
         * Returns the explanation string, fully formatted.  Only formats the string once.
         */
        @Override
        @Nullable
        public String getExplanation() {
            if (explanationString == null && explanation != null) {
                explanationString = String.format(Locale.ROOT, explanation, explanationParams);
            }
            return this.explanationString;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }

            if (object == null || getClass() != object.getClass()) {
                return false;
            }

            Decision.Single s = (Decision.Single) object;
            return this.type == s.type &&
                       Objects.equals(label, s.label) &&
                       Objects.equals(getExplanation(), s.getExplanation());
        }

        @Override
        public int hashCode() {
            int result = type.hashCode();
            result = 31 * result + (label == null ? 0 : label.hashCode());
            String explanationStr = getExplanation();
            result = 31 * result + (explanationStr == null ? 0 : explanationStr.hashCode());
            return result;
        }

        @Override
        public String toString() {
            if (explanationString != null || explanation != null) {
                return type + "(" + getExplanation() + ")";
            }
            return type + "()";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("decider", label);
            builder.field("decision", type);
            String explanation = getExplanation();
            builder.field("explanation", explanation != null ? explanation : "none");
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(false); // flag specifying its a single decision
            type.writeTo(out);
            out.writeOptionalString(label);
            // Flatten explanation on serialization, so that explanationParams
            // do not need to be serialized
            out.writeOptionalString(getExplanation());
        }
    }

    /**
     * Simple class representing a list of decisions
     */
    public static class Multi extends Decision {

        private final List<Decision> decisions = new ArrayList<>();

        /**
         * Add a decision to this {@link Multi}decision instance
         * @param decision {@link Decision} to add
         * @return {@link Multi}decision instance with the given decision added
         */
        public Multi add(Decision decision) {
            decisions.add(decision);
            return this;
        }

        @Override
        public Type type() {
            Type ret = Type.YES;
            for (int i = 0; i < decisions.size(); i++) {
                Type type = decisions.get(i).type();
                if (type == Type.NO) {
                    return type;
                } else if (type == Type.THROTTLE) {
                    ret = type;
                }
            }
            return ret;
        }

        @Override
        @Nullable
        public String label() {
            // Multi decisions have no labels
            return null;
        }

        @Override
        @Nullable
        public String getExplanation() {
            throw new UnsupportedOperationException("multi-level decisions do not have an explanation");
        }

        @Override
        public List<Decision> getDecisions() {
            return Collections.unmodifiableList(this.decisions);
        }

        @Override
        public boolean equals(final Object object) {
            if (this == object) {
                return true;
            }

            if (object == null || getClass() != object.getClass()) {
                return false;
            }

            final Decision.Multi m = (Decision.Multi) object;

            return this.decisions.equals(m.decisions);
        }

        @Override
        public int hashCode() {
            return 31 * decisions.hashCode();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (Decision decision : decisions) {
                sb.append("[").append(decision.toString()).append("]");
            }
            return sb.toString();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            for (Decision d : decisions) {
                d.toXContent(builder, params);
            }
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(true); // flag indicating it is a multi decision
            out.writeVInt(getDecisions().size());
            for (Decision d : getDecisions()) {
                d.writeTo(out);
            }
        }
    }
}
