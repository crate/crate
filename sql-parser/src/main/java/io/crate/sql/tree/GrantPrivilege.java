package io.crate.sql.tree;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class GrantPrivilege extends Statement {

    private final List<String> userNames;
    private final List<String> privilegeTypes;
    private final boolean all;

    public GrantPrivilege(List<String> userNames) {
        this.userNames = userNames;
        privilegeTypes = Collections.emptyList();
        all = true;
    }

    public GrantPrivilege(List<String> userNames, List<String> privilegeTypes) {
        this.userNames = userNames;
        this.privilegeTypes = privilegeTypes;
        this.all = false;
    }

    public List<String> privileges() {
        return privilegeTypes;
    }

    public List<String> userNames() {
        return userNames;
    }

    public boolean all() {
        return all;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGrantPrivilege(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GrantPrivilege that = (GrantPrivilege) o;
        return all == that.all &&
               Objects.equals(userNames, that.userNames) &&
               Objects.equals(privilegeTypes, that.privilegeTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userNames, privilegeTypes, all);
    }

    @Override
    public String toString() {
        return "GrantPrivilege{" +
               "allPrivileges=" + all +
               "privilegeTypes=" + privilegeTypes +
               ", userNames=" + userNames +
               '}';
    }
}
