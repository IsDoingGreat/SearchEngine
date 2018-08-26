package in.nimbo.isDoing.searchEngine.pipeline.Console;

import asg.cliche.ShellFactory;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.pipeline.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsolePipeline {
    private final static Logger logger = LoggerFactory.getLogger(ConsolePipeline.class);
    private static Output output = new ConsoleOutput();

    public void load() {
        try {
            Engine engine = Engine.start(output);

            ShellFactory.createConsoleShell("Search Engine", "Enter '?list' to list all commands",
                    new ConsoleCommands(engine)).commandLoop();


        } catch (Exception e) {
            logger.error("Program Exited With Error", e);
            System.out.println("There Were Some Problems! :(  Please See Log File For More Information");
        }
    }

}
