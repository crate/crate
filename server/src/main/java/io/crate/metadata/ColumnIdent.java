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
import java.util.NoSuchElementException;
import java.util.StringJoiner;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.common.StringUtils;
import io.crate.common.collections.LexicographicalOrdering;
import io.crate.common.collections.Lists;
import io.crate.exceptions.InvalidColumnNameException;
import io.crate.sql.Identifiers;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.Literal;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.SubscriptExpression;

public abstract sealed class ColumnIdent
    implements Comparable<ColumnIdent>, Accountable, Writeable
    permits ColumnIdent.Col0, ColumnIdent.ColN {

    private static final Comparator<ColumnIdent> COMPARATOR = Comparator.comparing(ColumnIdent::name)
        .thenComparing(ColumnIdent::path, new LexicographicalOrdering<>(Comparator.naturalOrder()));

    public static ColumnIdent of(StreamInput in) throws IOException {
        String name = in.readString();
        int numParts = in.readVInt();
        if (numParts == 0) {
            return new Col0(name);
        } else {
            ArrayList<String> path = new ArrayList<>(numParts);
            for (int i = 0; i < numParts; i++) {
                path.add(in.readString());
            }
            return new ColN(name, path);
        }
    }

    static final class Col0 extends ColumnIdent {

        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Col0.class);

        private final String name;

        public Col0(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public List<String> path() {
            return List.of();
        }

        @Override
        public boolean isRoot() {
            return true;
        }

        @Override
        public ColumnIdent getRoot() {
            return this;
        }

        @Override
        public ColumnIdent getChild(String childName) {
            return new ColN(name, List.of(childName));
        }

        @Override
        protected String sqlFqn(String name) {
            return name;
        }

        @Override
        public String fqn() {
            return name;
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Col0 other && name.equals(other.name);
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + RamUsageEstimator.sizeOf(name);
        }
    }

    static final class ColN extends ColumnIdent {

        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ColN.class);

        private final String name;
        private final List<String> path;

        public ColN(String name, List<String> path) {
            assert !path.isEmpty() : "Must use ColumnIdent0 instances if path is empty";
            this.name = name;
            this.path = path;
        }

        @Override
        public ColumnIdent getChild(String childName) {
            return new ColN(name, Lists.concat(path, childName));
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public List<String> path() {
            return path;
        }

        @Override
        public boolean isRoot() {
            return false;
        }

        @Override
        public ColumnIdent getRoot() {
            return new Col0(name);
        }

        @Override
        protected String sqlFqn(String name) {
            StringBuilder sb = new StringBuilder(name);
            for (String s : path()) {
                sb.append("['");
                sb.append(s);
                sb.append("']");
            }
            return sb.toString();
        }

        @Override
        public String fqn() {
            StringJoiner stringJoiner = new StringJoiner(".");
            stringJoiner.add(name());
            for (String p : path()) {
                stringJoiner.add(p);
            }
            return stringJoiner.toString();
        }

        @Override
        public int hashCode() {
            return 31 * name.hashCode() + 31 * path.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ColN other
                && name.equals(other.name)
                && path.equals(other.path);
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE
                + RamUsageEstimator.sizeOf(name)
                + path.stream().mapToLong(RamUsageEstimator::sizeOf).sum();
        }
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
        ColumnIdent columnIdent = path.isEmpty() ? new Col0(name) : new ColN(name, path);
        columnIdent.validForCreate();
        return columnIdent;
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
        return fromNameSafe(name, path);
    }

    public static ColumnIdent of(String name) {
        return new Col0(name);
    }

    public static ColumnIdent of(String name, String child) {
        return new ColN(name, List.of(child));
    }

    public static ColumnIdent of(String name, @Nullable List<String> path) {
        return path == null || path.isEmpty() ? new Col0(name) : new ColN(name, path);
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
        return parent.getChild(name);
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
            return new ColN(parts.get(0), parts.subList(1, parts.size()));
        } else {
            return new Col0(parts.get(0));
        }
    }

    public abstract ColumnIdent getChild(String childName);

    /**
     * Get the first non-map value from a map by traversing the name/path of the column
     */
    public static Object get(Map<?, ?> map, ColumnIdent column) {
        Object obj = map.get(column.name());
        if (obj instanceof Map<?, ?> m) {
            Object element = null;
            for (int i = 0; i < column.path().size(); i++) {
                element = m.get(column.path().get(i));
                if (element instanceof Map<?, ?> innerMap) {
                    m = innerMap;
                } else {
                    return element;
                }
            }
            return element;
        }
        return obj;
    }

    /**
     * Convert a ColumnIdent back into a parser expression
     **/
    public Expression toExpression() {
        Expression fqn = new QualifiedNameReference(QualifiedName.of(name()));
        for (String child : path()) {
            fqn = new SubscriptExpression(fqn, Literal.fromObject(child));
        }
        return fqn;
    }


    /**
     * Checks whether this ColumnIdent is a child of <code>parentIdent</code>
     *
     * @param parent the ident to check for parenthood
     *
     * @return true if <code>parentIdent</code> is parentIdent of this, false otherwise.
     */
    public boolean isChildOf(ColumnIdent parent) {
        if (!name().equals(parent.name())) {
            return false;
        }
        if (path().size() > parent.path().size()) {
            Iterator<String> parentIt = parent.path().iterator();
            Iterator<String> it = path().iterator();
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
     * Replaces the parent name of this column specified by the 'newName' e.g.::
     *   'a.b.c'.replacePrefix('a.x') -> 'a.x.c' : parent 'a.b' renamed to 'a.x' such that its child 'a.b.c' is renamed to 'a.x.c'
     *   'a.b.c'.replacePrefix('x') -> 'x.b.c' : parent 'a' renamed to 'x' such that its child 'a.b.c' is renamed to 'x.b.c'
     * This method also renames self e.g.::
     *   'a'.replacePrefix('b') -> 'b'
     */
    @NotNull
    public ColumnIdent replacePrefix(@NotNull ColumnIdent newName) {
        assert newName.path().isEmpty() || this.isChildOf(newName.getParent());
        List<String> replaced = new ArrayList<>(newName.path());
        replaced.addAll(this.path().subList(newName.path().size(), this.path().size()));
        return replaced.isEmpty() ? new Col0(newName.name()) : new ColN(newName.name(), replaced);
    }

    /**
     * Checks if column-name satisfies the criteria for naming a column or
     * throws an exception otherwise.
     *
     * @param columnName column name to check for validity
     */
    private static void validateColumnName(String columnName) {
        validateObjectKey(columnName);
        if (isSystemColumn(columnName)) {
            throw new InvalidColumnNameException(columnName, "conflicts with system column pattern");
        }
    }

    /**
     * Checks if column-name satisfies the criteria for naming an object column
     * or throws an exception otherwise.
     * This function differs from validate column name in terms of that all
     * names beginning with an underscore are allowed.
     *
     * @param columnName column name to check for validity
     */
    private static void validateObjectKey(String columnName) {
        if (columnName.indexOf('.') != -1) {
            throw new InvalidColumnNameException(columnName, "contains a dot");
        }
        if (columnName.contains("[")) {
            throw new InvalidColumnNameException(
                columnName, "conflicts with subscript pattern, square brackets are not allowed");
        }
        for (int i = 0; i < columnName.length(); i++) {
            switch (columnName.charAt(i)) {
                case '\t':
                case '\n':
                case '\r':
                    throw new InvalidColumnNameException(columnName, "contains illegal whitespace character");
                default:
            }
        }
    }

    /**
     * Returns true if the name is reserved for system columns.
     *
     * See {@link ColumnIdent#isSystemColumn()} for system column naming scheme.
     */
    private static boolean isSystemColumn(String name) {
        int length = name.length();
        if (length == 0) {
            return false;
        }
        if (name.charAt(0) != '_') {
            return false;
        }
        for (int i = 1; i < length; i++) {
            char ch = name.charAt(i);
            if (ch == '_' && (name.charAt(i - 1) == '_' || i + 1 == length)) {
                return false;
            }
            if (ch != '_' && (ch < 'a' || ch > 'z')) {
                return false;
            }
        }
        return true;
    }

    /**
     * person['addresses']['street'] --&gt; person['addresses']
     * <p>
     * person --&gt; null
     */
    @Nullable
    public ColumnIdent getParent() {
        switch (path().size()) {
            case 0:
                return null;

            case 1:
                return new Col0(name());

            default:
                return new ColN(name(), path().subList(0, path().size() - 1));
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
        switch (path().size()) {
            case 0:
                return null;

            case 1:
                return new Col0(path().get(0));

            default:
                return new ColN(path().get(0), path().subList(1, path().size()));
        }
    }

    /**
     * Returns true if this is a system column.
     *
     * System column naming rules:
     * <ul>
     * <li>Start with an _</li>
     * <li>Followed by one or more lowercase letters (a-z)</li>
     * <li>Can contain more _ between letters, but not successive, and not at the end</li>
     * </ul>
     *
     * <pre>
     *  _name    -> system column
     *  _foo_bar -> system column
     *  __name   -> no system column
     *  _name_   -> no system column
     * </pre>
     **/
    public boolean isSystemColumn() {
        return isSystemColumn(name());
    }

    /**
     * person['addresses']['street'] --&gt; person
     * <p>
     * person --&gt; person
     */
    public abstract ColumnIdent getRoot();

    public abstract String name();

    public abstract List<String> path();

    /**
     * @return true if this is a top level column, otherwise false
     */
    public abstract boolean isRoot();

    /**
     * This is the internal representation of a column identifier and may not supported by the SQL syntax.
     * For external exposure use {@link #sqlFqn()}.
     */
    public abstract String fqn();

    protected abstract String sqlFqn(String name);

    public String quotedOutputName() {
        return sqlFqn(Identifiers.quoteIfNeeded(name()));
    }

    public String sqlFqn() {
        return sqlFqn(name());
    }


    @Override
    public String toString() {
        return sqlFqn();
    }

    @Override
    public int compareTo(ColumnIdent o) {
        return COMPARATOR.compare(this, o);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name());
        out.writeVInt(path().size());
        for (String s : path()) {
            out.writeString(s);
        }
    }

    /**
     * Create a new ColumnIdent with the name inserted at the start
     * <p>
     * E.g. ColumnIdent y['z'].prepend('x') becomes ColumnIdent x['y']['z']
     */
    public ColumnIdent prepend(String name) {
        if (path().isEmpty()) {
            return new ColN(name, List.of(this.name()));
        }
        List<String> newPath = new ArrayList<>(path());
        newPath.add(0, this.name());
        return new ColN(name, newPath);
    }

    public String leafName() {
        if (path().isEmpty()) {
            return name();
        } else {
            return path().getLast();
        }
    }

    public Iterable<ColumnIdent> parents() {
        return () -> new Iterator<>() {

            int level = path().size();

            @Override
            public boolean hasNext() {
                return level > 0;
            }

            @Override
            public ColumnIdent next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("ColumnIdent has no more parents");
                }
                level--;
                List<String> newPath = path().subList(0, level);
                return newPath.isEmpty() ? new Col0(name()) : new ColN(name(), newPath);
            }
        };
    }

    public void validForCreate() {
        validateColumnName(name());
        path().forEach(x -> validateObjectKey(x));
    }

    public List<String> getRelativePath(ColumnIdent parent) {
        assert isChildOf(parent) : parent.sqlFqn() + " is not parent of: " + sqlFqn();
        List<String> newPath = new ArrayList<>();
        for (String p : path()) {
            if (parent.path().contains(p) == false) {
                newPath.add(p);
            }
        }
        return newPath;
    }
}
