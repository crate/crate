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

package io.crate.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.common.StringUtils;
import io.crate.common.collections.Lists2;
import io.crate.common.collections.Ordering;
import io.crate.exceptions.InvalidColumnNameException;
import io.crate.sql.Identifiers;
import io.crate.sql.tree.QualifiedName;

public class ColumnIdent implements Comparable<ColumnIdent>, Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ColumnIdent.class);
    private static final Pattern UNDERSCORE_PATTERN = Pattern.compile("^_([a-z][_a-z]*)*[a-z]$");

    private static final Comparator<Iterable<String>> ORDERING = Ordering.<String>natural().lexicographical();

    private final String name;
    private final List<String> path;

    public ColumnIdent(StreamInput in) throws IOException {
        name = in.readString();
        int numParts = in.readVInt();
        if (numParts > 0) {
            path = new ArrayList<>(numParts);
            for (int i = 0; i < numParts; i++) {
                path.add(in.readString());
            }
        } else {
            path = List.of();
        }
    }

    public ColumnIdent(String name) {
        this.name = name;
        this.path = List.of();
    }

    public ColumnIdent(String name, String childName) {
        this(name, List.of(childName));
    }

    public ColumnIdent(String name, @Nullable List<String> path) {
        this.name = name;
        this.path = Objects.requireNonNullElse(path, List.of());
    }

    /**
     * Creates and validates ident from given string.
     *
     * @param name ident name used for ColumnIdent creation
     * @param path path
     *
     * @return the validated ColumnIdent
     */
    public static ColumnIdent fromNameSafe(String name, List<String> path) {
        validateColumnName(name);
        for (String part : path) {
            validateObjectKey(part);
        }
        return new ColumnIdent(name, path);
    }

    public static ColumnIdent fromNameSafe(QualifiedName qualifiedName, @Nullable List<String> path) {
        path = path == null ? List.of() : path;
        List<String> parts = qualifiedName.getParts();
        if (parts.size() != 1) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "Column reference \"%s\" has too many parts. " +
                "A column must not have a schema or a table here.", qualifiedName));
        }
        String name = parts.get(0);
        validateColumnName(name);
        for (String part : path) {
            validateObjectKey(part);
        }
        return new ColumnIdent(name, path);
    }

    /**
     * Creates ColumnIdent for parent and validates + adds given child name to
     * existing path.
     *
     * @param parent ColumnIdent of parent
     * @param name path name of child
     *
     * @return the validated ColumnIdent
     */
    public static ColumnIdent getChildSafe(ColumnIdent parent, String name) {
        validateObjectKey(name);
        return getChild(parent, name);
    }

    /**
     * @param path Column name; With dot-notation to separate nested columns ('toplevel' or 'obj.child.x')
     * @return A ColumnIdent created from the path
     */
    public static ColumnIdent fromPath(@Nullable String path) {
        if (path == null) {
            return null;
        }
        List<String> parts = StringUtils.splitToList('.', path);
        if (parts.size() > 1) {
            return new ColumnIdent(parts.get(0), parts.subList(1, parts.size()));
        } else {
            return new ColumnIdent(parts.get(0));
        }
    }

    public static ColumnIdent getChild(ColumnIdent parent, String name) {
        if (parent.isTopLevel()) {
            return new ColumnIdent(parent.name, name);
        }
        ArrayList<String> childPath = new ArrayList<>();
        childPath.addAll(parent.path);
        childPath.add(name);
        return new ColumnIdent(parent.name, childPath);
    }

    public ColumnIdent getChild(String name) {
        return ColumnIdent.getChild(this, name);
    }

    /**
     * Checks whether this ColumnIdent is a child of <code>parentIdent</code>
     *
     * @param parent the ident to check for parenthood
     *
     * @return true if <code>parentIdent</code> is parentIdent of this, false otherwise.
     */
    public boolean isChildOf(ColumnIdent parent) {
        if (!name.equals(parent.name)) {
            return false;
        }
        if (path.size() > parent.path.size()) {
            Iterator<String> parentIt = parent.path.iterator();
            Iterator<String> it = path.iterator();
            while (parentIt.hasNext()) {
                if (!parentIt.next().equals(it.next())) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Checks if column-name satisfies the criteria for naming a column or
     * throws an exception otherwise.
     *
     * @param columnName column name to check for validity
     */
    public static void validateColumnName(String columnName) {
        validateDotInColumnName(columnName);
        validateSubscriptPatternInColumnName(columnName);
        validateUnderscorePatternInColumnName(columnName);
    }

    /**
     * Checks if column-name satisfies the criteria for naming an object column
     * or throws an exception otherwise.
     * This function differs from validate column name in terms of that all
     * names beginning with an underscore are allowed.
     *
     * @param columnName column name to check for validity
     */
    public static void validateObjectKey(String columnName) {
        validateDotInColumnName(columnName);
        validateSubscriptPatternInColumnName(columnName);
    }

    /**
     * Checks if a column name contains a dot and throws an exception if it does.
     *
     * @param columnName column name to check for validity
     */
    private static void validateDotInColumnName(String columnName) {
        if (columnName.indexOf('.') != -1) {
            throw new InvalidColumnNameException(columnName, "contains a dot");
        }
    }

    /**
     * Checks if a column name contains a subscript notation and throws an exception if it does.
     *
     * @param columnName column name to check for validity
     */
    private static void validateSubscriptPatternInColumnName(String columnName) {
        if (columnName.contains("[")) {
            throw new InvalidColumnNameException(
                columnName, "conflicts with subscript pattern, square brackets are not allowed");
        }
    }

    /**
     * Checks if a column name contains a underscore pattern and throws an exception if it does.
     *
     * @param columnName column name to check for validity
     */
    private static void validateUnderscorePatternInColumnName(String columnName) {
        if (UNDERSCORE_PATTERN.matcher(columnName).matches()) {
            throw new InvalidColumnNameException(columnName, "conflicts with system column pattern");
        }
    }

    /**
     * person['addresses']['street'] --&gt; person['addresses']
     * <p>
     * person --&gt; null
     */
    @Nullable
    public ColumnIdent getParent() {
        switch (path.size()) {
            case 0:
                return null;

            case 1:
                return new ColumnIdent(name);

            default:
                return new ColumnIdent(name, path.subList(0, path.size() - 1));
        }
    }

    /**
     * creates a new columnName which just consists of the path of the given columnName
     * e.g.
     * <pre>foo['x']['y']</pre>
     * becomes
     * <pre> x['y']</pre>
     *
     * If the columnName doesn't have a path the return value is null
     */
    @Nullable
    public ColumnIdent shiftRight() {
        switch (path.size()) {
            case 0:
                return null;

            case 1:
                return new ColumnIdent(path.get(0));

            default:
                return new ColumnIdent(path.get(0), path.subList(1, path.size()));
        }
    }

    /**
     * returns true if this is a system column
     */
    public boolean isSystemColumn() {
        return UNDERSCORE_PATTERN.matcher(name).matches();
    }

    /**
     * person['addresses']['street'] --&gt; person
     * <p>
     * person --&gt; person
     */
    public ColumnIdent getRoot() {
        if (isTopLevel()) {
            return this;
        }
        return new ColumnIdent(name());
    }

    public String name() {
        return name;
    }

    /**
     * This is the internal representation of a column identifier and may not supported by the SQL syntax.
     * For external exposure use {@link #sqlFqn()}.
     */
    public String fqn() {
        if (isTopLevel()) {
            return name;
        }
        StringJoiner stringJoiner = new StringJoiner(".");
        stringJoiner.add(name);
        for (String p : path) {
            stringJoiner.add(p);
        }
        return stringJoiner.toString();
    }

    public String quotedOutputName() {
        return sqlFqn(Identifiers.quoteIfNeeded(name));
    }

    public String sqlFqn() {
        return sqlFqn(name);
    }

    private String sqlFqn(String name) {
        StringBuilder sb = new StringBuilder(name);
        for (String s : path) {
            sb.append("['");
            sb.append(s);
            sb.append("']");
        }
        return sb.toString();
    }

    public List<String> path() {
        return path;
    }

    /**
     * @return true if this is a top level column, otherwise false
     */
    public boolean isTopLevel() {
        return path.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnIdent that = (ColumnIdent) o;

        if (!name.equals(that.name)) return false;
        return path.equals(that.path);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + path.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return sqlFqn();
    }

    @Override
    public int compareTo(ColumnIdent o) {
        return Comparator.comparing(ColumnIdent::name)
            .thenComparing(ColumnIdent::path, ORDERING)
            .compare(this, o);
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVInt(path.size());
        for (String s : path) {
            out.writeString(s);
        }
    }

    /**
     * Create a new ColumnIdent with the name inserted at the start
     * <p>
     * E.g. ColumnIdent y['z'].prepend('x') becomes ColumnIdent x['y']['z']
     */
    public ColumnIdent prepend(String name) {
        if (path.isEmpty()) {
            return new ColumnIdent(name, this.name);
        }
        List<String> newPath = new ArrayList<>(path);
        newPath.add(0, this.name);
        return new ColumnIdent(name, newPath);
    }

    public String leafName() {
        if (path.isEmpty()) {
            return name;
        } else {
            return path.get(path.size() - 1);
        }
    }

    public ColumnIdent append(String childName) {
        if (path.isEmpty()) {
            return new ColumnIdent(name, childName);
        } else {
            return new ColumnIdent(name, Lists2.concat(path, childName));
        }
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE
            + RamUsageEstimator.sizeOf(name)
            + path.stream().mapToLong(RamUsageEstimator::sizeOf).sum();
    }
}
