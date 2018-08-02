package in.nimbo.isDoing.searchEngine.pipeline;

import asg.cliche.Command;
import asg.cliche.ShellFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsolePipeline {
    private final static Logger logger = LoggerFactory.getLogger(ConsolePipeline.class);
    private Output output = new ConsoleOutput();

    public static void load() {
        try {
            ShellFactory.createConsoleShell("Search Engine", "Enter '?list' to list all commands",
                    new ConsolePipeline()).commandLoop();

        } catch (Exception e) {
            logger.error("Program Exited With Error", e);
            System.out.println("There Were Some Problems! :(  Please See Log File For More Information");
        }
    }

    @Command
    public void start() {
        // TODO: 8/2/18  
    }
}
