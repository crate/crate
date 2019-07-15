/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.sql.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class IntervalLiteral extends Literal {

    public enum Sign {
        POSITIVE {
            @Override
            public int multiplier() {
                return 1;
            }
        },
        NEGATIVE {
            @Override
            public int multiplier() {
                return -1;
            }
        };

        public abstract int multiplier();
    }

    public enum IntervalField {
        YEAR, MONTH, DAY, HOUR, MINUTE, SECOND
    }

    private final String value;
    private final Sign sign;
    private final IntervalField startField;
    private final Optional<IntervalField> endField;


    public IntervalLiteral(String value, Sign sign, IntervalField startField, Optional<IntervalField> endField) {
        checkNotNull(value, "value is null");
        checkNotNull(sign, "sign is null");
        checkNotNull(startField, "startField is null");
        checkNotNull(endField, "endField is null");

        this.value = value;
        this.sign = sign;
        this.startField = startField;
        this.endField = endField;
    }

    public String getValue() {
        return value;
    }

    public Sign getSign() {
        return sign;
    }

    public IntervalField getStartField() {
        return startField;
    }

    public Optional<IntervalField> getEndField() {
        return endField;
    }

    public boolean isYearToMonth() {
        return startField == IntervalField.YEAR || startField == IntervalField.MONTH;
    }


    @Override
    public int hashCode() {
        return Objects.hash(value, sign, startField, endField);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        IntervalLiteral other = (IntervalLiteral) obj;
        return Objects.equals(this.value, other.value) &&
               Objects.equals(this.sign, other.sign) &&
               Objects.equals(this.startField, other.startField) &&
               Objects.equals(this.endField, other.endField);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitIntervalLiteral(this, context);
    }
}
