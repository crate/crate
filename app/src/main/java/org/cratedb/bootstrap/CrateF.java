package org.cratedb.bootstrap;

public class CrateF extends Crate {

    public static void main(String[] args) {
        System.setProperty("es.foreground", "yes");
        Crate.main(args);
    }
}
