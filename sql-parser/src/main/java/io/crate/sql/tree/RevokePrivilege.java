package io.crate.sql.tree;

import java.util.List;


public class RevokePrivilege extends PrivilegeStatement {

    public RevokePrivilege(List<String> userNames, String clazz, List<QualifiedName> tableOrSchemaNames) {
        super(userNames, clazz, tableOrSchemaNames);
    }

    public RevokePrivilege(List<String> userNames, List<String> privilegeTypes, String clazz, List<QualifiedName> tableOrSchemaNames) {
        super(userNames, privilegeTypes, clazz, tableOrSchemaNames);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRevokePrivilege(this, context);
    }

    @Override
    public String toString() {
        return "RevokePrivilege{" +
               "allPrivileges=" + all +
               "privilegeTypes=" + privilegeTypes +
               ", userNames=" + userNames +
               '}';
    }
}
