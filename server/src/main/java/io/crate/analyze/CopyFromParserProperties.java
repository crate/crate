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

package io.crate.analyze;

import io.crate.common.annotations.VisibleForTesting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.Objects;

import static io.crate.analyze.CopyStatementSettings.CSV_COLUMN_SEPARATOR;
import static io.crate.analyze.CopyStatementSettings.EMPTY_STRING_AS_NULL;


public class CopyFromParserProperties implements Writeable {

    public static final CopyFromParserProperties DEFAULT = CopyFromParserProperties.of(Settings.EMPTY);

    private final boolean emptyStringAsNull;
    private final char columnSeparator;

    public static CopyFromParserProperties of(Settings settings) {
        return new CopyFromParserProperties(
            EMPTY_STRING_AS_NULL.get(settings),
            CSV_COLUMN_SEPARATOR.get(settings)
        );
    }

    @VisibleForTesting
    public CopyFromParserProperties(boolean emptyStringAsNull,
                                    char columnSeparator) {
        this.emptyStringAsNull = emptyStringAsNull;
        this.columnSeparator = columnSeparator;
    }

    public CopyFromParserProperties(StreamInput in) throws IOException {
        emptyStringAsNull = in.readBoolean();
        columnSeparator = (char) in.readByte();
    }

    public boolean emptyStringAsNull() {
        return emptyStringAsNull;
    }

    public char columnSeparator() {
        return columnSeparator;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(emptyStringAsNull);
        out.writeByte((byte) columnSeparator);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CopyFromParserProperties that = (CopyFromParserProperties) o;
        return emptyStringAsNull == that.emptyStringAsNull &&
               columnSeparator == that.columnSeparator;
    }

    @Override
    public int hashCode() {
        return Objects.hash(emptyStringAsNull, columnSeparator);
    }

    @Override
    public String toString() {
        return "CopyFromParserProperties{" +
               "emptyStringAsNull=" + emptyStringAsNull +
               ", columnSeparator=" + columnSeparator +
               '}';
    }
}
