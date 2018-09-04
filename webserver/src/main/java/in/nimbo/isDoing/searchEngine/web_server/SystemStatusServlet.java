package in.nimbo.isDoing.searchEngine.web_server;

import in.nimbo.isDoing.searchEngine.elastic.ElasticClient;
import in.nimbo.isDoing.searchEngine.hbase.HBaseClient;
import in.nimbo.isDoing.searchEngine.kafka.KafkaAdmin;
import org.codehaus.jackson.map.ObjectMapper;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SystemStatusServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Map<String, Object> dataMap = new HashMap<>();
        String pathInfo = req.getPathInfo();
        String requestedService = pathInfo == null ? "" : pathInfo.substring(1);
        if (requestedService.equals("elastic") || requestedService.isEmpty()) {
            try {
                dataMap.put("elastic", ElasticClient.getInstance().getJson());
            } catch (Exception e) {
                dataMap.putIfAbsent("errors", new ArrayList<>());
                ((List) dataMap.get("errors")).add(e.getMessage() == null ? "Unknown Error" : e.getMessage());
            }
        }

        if (requestedService.equals("hbase") || requestedService.isEmpty()) {
            try {
                dataMap.put("hbase", HBaseClient.getInstance().getJson());
            } catch (Throwable e) {
                dataMap.putIfAbsent("errors", new ArrayList<>());
                ((List) dataMap.get("errors")).add(e.getMessage() == null ? "Unknown Error" : e.getMessage());
            }
        }

        if (requestedService.equals("kafka") || requestedService.isEmpty()) {
            try {
                dataMap.put("kafka", KafkaAdmin.getJson());
            } catch (Exception e) {
                dataMap.putIfAbsent("errors", new ArrayList<>());
                ((List) dataMap.get("errors")).add(e.getMessage() == null ? "Unknown Error" : e.getMessage());
            }
        }

        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(dataMap);
        resp.setContentType("application/json");
        resp.setContentLength(json.getBytes().length);
        resp.getWriter().print(json);
    }
}