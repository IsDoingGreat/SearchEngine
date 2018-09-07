package in.nimbo.isDoing.searchEngine.crawler.server;

import in.nimbo.isDoing.searchEngine.crawler.CrawlerService;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import org.codehaus.jackson.map.ObjectMapper;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

public class StatusServlet extends HttpServlet {
    @Override
    public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        CrawlerService crawler = (CrawlerService) Engine.getInstance().getService("crawler");
        Map<String, Object> status = crawler.status();
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(status);
        resp.setContentType("application/json");
        resp.setContentLength(json.getBytes().length);
        resp.getWriter().write(json);
    }
}
