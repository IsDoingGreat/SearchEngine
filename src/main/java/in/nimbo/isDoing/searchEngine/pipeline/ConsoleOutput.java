package in.nimbo.isDoing.searchEngine.pipeline;

public class ConsoleOutput implements Output {
    @Override
    public void show(String object) {
        show(Type.INFO, object);
    }

    @Override
    public void show(Type type, String object) {
        if (type == Type.INFO)
            System.out.println(object);
        else
            System.err.println(object);
    }
}
