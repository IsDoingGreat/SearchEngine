package in.nimbo.isDoing.searchEngine.pipeline.Console;

import asg.cliche.ShellFactory;
import in.nimbo.isDoing.searchEngine.elastic.ElasticClient;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.hbase.HBaseClient;
import in.nimbo.isDoing.searchEngine.pipeline.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static in.nimbo.isDoing.searchEngine.pipeline.Output.Type.ERROR;

public class ConsolePipeline {
    private final static Logger logger = LoggerFactory.getLogger(ConsolePipeline.class);
    private static Output output = new ConsoleOutput();

    public void load() {
        try {
            Engine engine = Engine.start(output);

            ShellFactory.createConsoleShell("Search Engine", "Enter '?list' to list all commands",
                    new ConsoleCommands(engine)).commandLoop();

            Engine.getInstance().stopAll();
            try {
                ElasticClient.close();
            } catch (IOException e) {
                logger.error("Closing Elastic With Error", e);
                Engine.getOutput().show(ERROR, "Closing Elastic With Error");
            }

            try {
                HBaseClient.close();
            } catch (IOException e) {
                logger.error("Closing HBase With Error", e);
                Engine.getOutput().show(ERROR, "Closing HBase With Error");
            }

        } catch (Exception e) {
            logger.error("Program Exited With Error", e);
            System.out.println("There Were Some Problems! :(  Please See Log File For More Information");
        }
    }

}
