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

import org.jetbrains.annotations.VisibleForTesting;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.Objects;

import static io.crate.analyze.CopyStatementSettings.CSV_COLUMN_SEPARATOR;
import static io.crate.analyze.CopyStatementSettings.SKIP_NUM_LINES;
import static io.crate.analyze.CopyStatementSettings.EMPTY_STRING_AS_NULL;
import static io.crate.analyze.CopyStatementSettings.INPUT_HEADER_SETTINGS;


public class CopyFromParserProperties implements Writeable {

    public static final CopyFromParserProperties DEFAULT = CopyFromParserProperties.of(Settings.EMPTY);

    private final boolean emptyStringAsNull;
    private final char columnSeparator;
    private final boolean fileHeader;
    private final long skipNumLines;


    public static CopyFromParserProperties of(Settings settings) {
        return new CopyFromParserProperties(
            EMPTY_STRING_AS_NULL.get(settings),
            INPUT_HEADER_SETTINGS.get(settings),
            CSV_COLUMN_SEPARATOR.get(settings),
            SKIP_NUM_LINES.get(settings)
        );
    }

    @VisibleForTesting
    public CopyFromParserProperties(boolean emptyStringAsNull,
                                    boolean fileHeader,
                                    char columnSeparator,
                                    long skipNumLines) {
        this.emptyStringAsNull = emptyStringAsNull;
        this.fileHeader = fileHeader;
        this.columnSeparator = columnSeparator;
        this.skipNumLines = skipNumLines;
    }

    public CopyFromParserProperties(StreamInput in) throws IOException {
        emptyStringAsNull = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_4_8_0)) {
            fileHeader = in.readBoolean();
        } else {
            fileHeader = true;
        }
        columnSeparator = (char) in.readByte();
        if (in.getVersion().onOrAfter(Version.V_5_2_0)) {
            skipNumLines = in.readLong();
        } else {
            skipNumLines = 0L;
        }
    }

    public boolean emptyStringAsNull() {
        return emptyStringAsNull;
    }

    public boolean fileHeader() {
        return fileHeader;
    }

    public char columnSeparator() {
        return columnSeparator;
    }

    public long skipNumLines() {
        return skipNumLines;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(emptyStringAsNull);
        if (out.getVersion().onOrAfter(Version.V_4_8_0)) {
            out.writeBoolean(fileHeader);
        }
        out.writeByte((byte) columnSeparator);
        if (out.getVersion().onOrAfter(Version.V_5_2_0)) {
            out.writeLong(skipNumLines);
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
        CopyFromParserProperties that = (CopyFromParserProperties) o;
        return emptyStringAsNull == that.emptyStringAsNull &&
               fileHeader == that.fileHeader &&
               columnSeparator == that.columnSeparator &&
               skipNumLines == that.skipNumLines;
    }

    @Override
    public int hashCode() {
        return Objects.hash(emptyStringAsNull, fileHeader, columnSeparator, skipNumLines);
    }

    @Override
    public String toString() {
        return "CopyFromParserProperties{" +
               "emptyStringAsNull=" + emptyStringAsNull +
               ", fileHeader=" + fileHeader +
               ", columnSeparator=" + columnSeparator +
               ", skipNumLines=" + skipNumLines +
               '}';
    }
}
