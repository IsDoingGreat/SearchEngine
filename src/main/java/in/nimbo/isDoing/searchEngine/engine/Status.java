package in.nimbo.isDoing.searchEngine.engine;

import in.nimbo.isDoing.searchEngine.engine.interfaces.HaveStatus;

public class Status {
    public static Status get(Object object) {
        if (object instanceof HaveStatus)
            return ((HaveStatus) object).status();
        else
            return null;
    }
}
