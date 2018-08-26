package in.nimbo.isDoing.searchEngine.pipeline;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.pipeline.Console.ConsoleOutput;

public class BackgroundPipeline {
    public void load(String[] services) throws Exception {
        Engine.start(new ConsoleOutput());
        for (String service : services) {
            try {
//                Engine.getInstance().startService(service);
            } catch (Exception e) {
                Engine.getOutput().show(Output.Type.ERROR, "Error During Starting Service");
                e.printStackTrace();
            }
        }
    }
}
