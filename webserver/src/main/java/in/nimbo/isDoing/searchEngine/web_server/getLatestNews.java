package in.nimbo.isDoing.searchEngine.web_server;

import in.nimbo.isDoing.searchEngine.elastic.ElasticClient;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.SystemConfigs;
import in.nimbo.isDoing.searchEngine.pipeline.Console.ConsoleOutput;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class getLatestNews extends HttpServlet {
    public static void main(String[] args) throws Exception {
        Engine.start(new ConsoleOutput(), new SystemConfigs("crawler"));
        for (int i = 0; i < 100; i++) {
            IndexRequest index = new IndexRequest("news", "_doc");
            index.source(
                    "title", "Title" + i,
                    "category", "sport",
                    "url", "http://google.com",
                    "date", new Date(),
                    "text", "asdasdads" + i
            );
            ElasticClient.getClient().index(index);
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        Map<String, Object> map = new HashMap<>();
        try {
            SearchRequest searchRequest = new SearchRequest("news");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.rangeQuery("date").gte("now-1d/m"));
            searchRequest.source(searchSourceBuilder);
            SearchResponse search = ElasticClient.getClient().search(searchRequest);
            List<Map> resultList = new ArrayList<>();
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");
            for (SearchHit hit : search.getHits()) {
                Map<String, Object> data = hit.getSourceAsMap();
//                System.out.println(data.get("date").getClass());
//                data.computeIfPresent("date", (s, o) -> format.format(o));
                data.computeIfPresent("text", (s, o) -> ((String) o).length() > 155 ? ((String) o).substring(0, 150) : o);
                resultList.add(data);
            }

            if (resultList.isEmpty()) {
                map.putIfAbsent("errors", new ArrayList<>());
                ((List) map.get("errors")).add("No Result!");
            } else {
                map.put("result", resultList);
            }
        } catch (Exception e) {
            map.putIfAbsent("errors", new ArrayList<>());
            ((List) map.get("errors")).add(e.getMessage());
        }

        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(map);
        resp.setContentType("application/json");
        resp.setContentLength(json.getBytes().length);
        resp.getWriter().print(json);
    }
}
