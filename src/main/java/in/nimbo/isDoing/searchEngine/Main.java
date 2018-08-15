package in.nimbo.isDoing.searchEngine;

import in.nimbo.isDoing.searchEngine.pipeline.BackgroundPipeline;
import in.nimbo.isDoing.searchEngine.pipeline.Console.ConsolePipeline;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length > 0)
            new BackgroundPipeline().load(args);
        else
            new ConsolePipeline().load();
    }
}
