package in.nimbo.isDoing.searchEngine.web_server.controller;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public interface WebController {
    void handle(HttpServletRequest request, HttpServletResponse response) throws IOException;
}
