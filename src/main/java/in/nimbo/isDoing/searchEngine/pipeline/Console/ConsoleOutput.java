package in.nimbo.isDoing.searchEngine.pipeline.Console;

import in.nimbo.isDoing.searchEngine.pipeline.Output;

public class ConsoleOutput implements Output {
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";

    @Override
    public void show(String object) {
        show(Type.INFO, object);
    }

    @Override
    public void show(Type type, String object) {
        if (type == Type.INFO)
            System.out.println(ANSI_BLUE + "[INFO]" + ANSI_RESET + "  " + object);
        else
            System.err.println(ANSI_RED + "[ERROR]" + ANSI_RESET + "  " + object);
    }
}
