package com.beancount.jdbc;

public final class Version {
    static final int MAJOR = 0;
    static final int MINOR = 4;
    static final int PATCH = 25;
    private static final String QUALIFIER = "alpha";

    public static final String FULL = MAJOR + "." + MINOR + "." + PATCH + "-" + QUALIFIER;
    public static final String CALCITE_VERSION = "1.38.0";
    public static final String RUNTIME = FULL + " (Calcite " + CALCITE_VERSION + ")";

    private Version() {}
}
