package in.nimbo.isDoing.searchEngine.pipeline;

public class ConsoleOutput implements Output {
    @Override
    public void out(String object) {
        out(Type.INFO, object);
    }

    @Override
    public void out(Type type, String object) {
        if (type == Type.INFO)
            System.out.println(object);
        else
            System.err.println(object);
    }
}
