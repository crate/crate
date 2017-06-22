package io.crate.sql.tree;

import java.util.List;


public class GrantPrivilege extends PrivilegeStatement {

    public GrantPrivilege(List<String> userNames, String clazz, List<QualifiedName> tableOrSchemaNames) {
        super(userNames, clazz, tableOrSchemaNames);
    }

    public GrantPrivilege(List<String> userNames, List<String> privilegeTypes, String clazz, List<QualifiedName> tableOrSchemaNames) {
        super(userNames, privilegeTypes, clazz, tableOrSchemaNames);
    }


    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGrantPrivilege(this, context);
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
