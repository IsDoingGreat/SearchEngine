package in.nimbo.isDoing.searchEngine.web_server;

import in.nimbo.isDoing.searchEngine.hbase.HBaseClient;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ShowNewsTrendsServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Map<String, Object> map = new HashMap<>();
        try {
            int passHour = 1;
            int wordsLimit = 10;
            if (!(req.getParameter("h") == null || req.getParameter("h").isEmpty())) {
                passHour = Integer.parseInt(req.getParameter("h"));
            }
            if (!(req.getParameter("s") == null || req.getParameter("s").isEmpty())) {
                wordsLimit = Integer.parseInt(req.getParameter("s"));
            }
            Connection connection = HBaseClient.getConnection();

            TableName tn = TableName.valueOf("newsTrendWords");
            Map<String, Integer> twitterTrendWords = new HashMap<>();

            Table table = connection.getTable(tn);
            Scan scan = new Scan();
            Date date = new Date();
            scan.setTimeRange(date.getTime() - passHour * 3600 * 1000, date.getTime());
            scan.addColumn(Bytes.toBytes("wordCount"), Bytes.toBytes("count"));

            for (Result row : table.getScanner(scan)) {
                String word = Bytes.toString(row.getRow());
                int count = Bytes.toInt(row.getValue(Bytes.toBytes("wordCount"), Bytes.toBytes("count")));

                if (!twitterTrendWords.containsKey(word)) {
                    twitterTrendWords.put(word, count);
                }
            }
            table.close();
            List<Map.Entry<String, Integer>> list =
                    new LinkedList<>(twitterTrendWords.entrySet());
            List<Map.Entry<String, Integer>> entries = list.stream()
                    .sorted(Comparator.comparing((Function<Map.Entry<String, Integer>, Integer>) Map.Entry::getValue).reversed())
                    .limit(wordsLimit).collect(Collectors.toList());

            map.put("result", entries);
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