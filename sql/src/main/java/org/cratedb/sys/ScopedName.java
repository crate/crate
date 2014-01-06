package org.cratedb.sys;

import java.util.Arrays;

/**
 * A scoped name represents an identifier in a given scope in the system
 */
public class ScopedName {

    private final Scope scope;
    private final String[] path;
    private final int hashCode;

    public ScopedName(Scope scope, String... path) {
        this(scope, createHashCode(scope, path), path);
    }

    public ScopedName(Scope scope, int hashCode, String... path) {
        this.scope = scope;
        this.path = path;
        this.hashCode = hashCode;
    }

    public Scope scope() {
        return scope;
    }

    public String[] path() {
        return path;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private static int createHashCode(Scope scope, String[] path) {
        int result = scope.hashCode();
        if (path != null) {
            for (String name : path)
                result = 31 * result + (name == null ? 0 : name.hashCode());
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ScopedName) {
            ScopedName other = (ScopedName) o;
            if (other.scope() == scope) {
                return Arrays.equals(other.path(), path);
            }
        }
        return false;
    }


}
