package in.nimbo.isDoing.searchEngine.pipeline.File;

import in.nimbo.isDoing.searchEngine.pipeline.Output;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileOutput implements Output {
    private FileWriter fw;
    private BufferedWriter bw;
    private PrintWriter out;

    public FileOutput() throws IOException {
        Path path = Paths.get("./output.log");
        fw = new FileWriter(path.toFile(), true);
        bw = new BufferedWriter(fw);
        out = new PrintWriter(bw);
    }

    @Override
    public void show(String object) {
        show(Type.INFO, object);
    }

    @Override
    public void show(Type type, String object) {
        out.println("[" + type.toString() + "]  " + object);
        out.flush();
    }

}
