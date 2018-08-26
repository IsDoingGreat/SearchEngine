package in.nimbo.isDoing.searchEngine.pipeline.Console;

import asg.cliche.Command;
import asg.cliche.Param;
import in.nimbo.isDoing.searchEngine.engine.Engine;

public class ConsoleCommands {
    private Engine engine;

    public ConsoleCommands(Engine engine) {
        this.engine = engine;
    }


    @Command
    public void stop(@Param(name = "Service Name") String serviceName) {
        engine.stopService(serviceName);
    }

    @Command
    public void status(@Param(name = "Service Name") String serviceName) {
        engine.status(serviceName);
    }
}
