package in.nimbo.isDoing.searchEngine.pipeline.Console;

import in.nimbo.isDoing.searchEngine.engine.Status;
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
        else if (type == Type.ERROR)
            System.out.println(ANSI_RED + "[ERROR]" + ANSI_RESET + "  " + object);
        else if (type == Type.WARN)
            System.out.println(ANSI_YELLOW + "[WARN]" + ANSI_RESET + "  " + object);
        else if (type == Type.SUCCESS)
            System.out.println(ANSI_GREEN + "[SUCCESS]" + ANSI_RESET + "  " + object);
        else if (type == Type.STATUS)
            System.out.println(ANSI_CYAN + "[STATUS]" + ANSI_RESET + "  " + object);

    }

    private void show(Status status, int depth) {
        if (status == null)
            return;
        
        String tabs = new String(new char[depth]).replace("\0", "\t");
        show(Type.STATUS, tabs + status.getTitle() + " : " + status.getDescription());

        for (String line : status.getLines()) {
            System.out.println(tabs + line);
        }

        for (Status subSection : status.getSubSections()) {
            show(subSection, depth + 1);
        }
        System.out.println();
    }

    @Override
    public void show(Status status) {
        show(status, 0);
    }


}
