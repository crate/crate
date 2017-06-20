package io.crate.sql.tree;

import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

public class GrantPrivilege extends Statement {

    private final EnumSet<PrivilegeType> privilegeTypes;
    private final List<String> userNames;

    public GrantPrivilege(List<String> userNames, EnumSet<PrivilegeType> privilegeTypes) {
        this.privilegeTypes = privilegeTypes;
        this.userNames = userNames;
    }

    public EnumSet<PrivilegeType> privileges() {
        return privilegeTypes;
    }

    public List<String> userNames() {
        return userNames;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGrantPrivilege(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final GrantPrivilege that = (GrantPrivilege) o;
        return Objects.equals(this.privilegeTypes, that.privilegeTypes)
            && Objects.equals(this.userNames, that.userNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(privilegeTypes, userNames);
    }

    @Override
    public String toString() {
        return "GrantPrivilege{" +
               "privilegeTypes=" + privilegeTypes +
               ", userNames=" + userNames +
               '}';
    }

    @Override
    public PrivilegeType privilegeType() {
        return PrivilegeType.DCL;
    }

    @Override
    public PrivilegeClazz privilegeClazz() {
        return PrivilegeClazz.CLUSTER;
    }

    @Override
    public String privilegeIdent() {
        return null;
    }
}
