package in.nimbo.isDoing.searchEngine.pipeline.File;

import in.nimbo.isDoing.searchEngine.engine.Status;
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

    private void show(Status status, int depth) {
        if (status == null)
            return;

        String tabs = new String(new char[depth]).replace("\0", "\t");
        show(Type.STATUS, tabs + status.getTitle() + " : " + status.getDescription());

        for (String line : status.getLines()) {
            out.println(tabs + line);
            out.flush();
        }

        for (Status subSection : status.getSubSections()) {
            show(subSection, depth + 1);
        }
        out.println();
    }

    @Override
    public void show(Status status) {
        show(status, 0);
    }
}
