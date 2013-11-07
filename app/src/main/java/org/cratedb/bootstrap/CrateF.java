package org.cratedb.bootstrap;

/**
 * A main entry point when starting from the command line.
 */
public class CrateF extends Crate {

    public static void main(String[] args) {
        System.setProperty("es.foreground", "yes");
        Crate.main(args);
    }
}
