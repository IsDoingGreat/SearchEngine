package in.nimbo.isDoing.searchEngine.web_server;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.SystemConfigs;
import in.nimbo.isDoing.searchEngine.hbase.HBaseClient;
import in.nimbo.isDoing.searchEngine.pipeline.Console.ConsoleOutput;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class HostToHostGraph extends HttpServlet {
    public static void main(String[] args) throws Exception {
        Engine.start(new ConsoleOutput(), new SystemConfigs("crawler"));
        Connection connection = HBaseClient.getConnection();
        TableName tn = TableName.valueOf("hostToHost");
        Table table = connection.getTable(tn);
        List<String> list = Files.readAllLines(Paths.get("./seeds.txt"));
        Iterator<String> iterator = list.iterator();
        iterator.next();
        for (int i = 0; i < 1000; i++) {
            Put p = new Put(Bytes.toBytes(new URL(iterator.next()).getHost()));
            for (int j = 0; j <= 25; j++) {
                p.addColumn(Bytes.toBytes("I"), Bytes.toBytes(new URL(iterator.next()).getHost()), Bytes.toBytes(((int) (Math.random() * 1000))));
            }
            table.put(p);
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        Map<String, Object> map = new HashMap<>();
        String requestedHost = req.getParameter("host");
        if (requestedHost == null || requestedHost.isEmpty()) {
            map.putIfAbsent("errors", new ArrayList<>());
            ((List) map.get("errors")).add("Please specify a query");
        } else {
            Map<String, Integer> hostIDs = new HashMap<>();
            AtomicInteger idCount = new AtomicInteger(0);
            List<Edge> edgeList = new ArrayList<>();
            List<Host> hostList = new ArrayList<>();

            Connection connection = HBaseClient.getConnection();
            TableName tn = TableName.valueOf("hostToHost");
            Table table = connection.getTable(tn);

            Get requestedHostGet = new Get(Bytes.toBytes(requestedHost));
            requestedHostGet.addFamily(Bytes.toBytes("info"));

            Result result = table.get(requestedHostGet);

            if (result.isEmpty()) {
                map.putIfAbsent("errors", new ArrayList<>());
                ((List) map.get("errors")).add("No Result!");
            } else {
                hostIDs.computeIfAbsent(requestedHost, s -> idCount.incrementAndGet());
                List<Cell> cells = result.listCells();
                for (int i = 0; i < cells.size(); i++) {
                    String host = Bytes.toString(CellUtil.cloneQualifier(cells.get(i)));
                    int value = Bytes.toInt(CellUtil.cloneValue(cells.get(i)));
                    hostIDs.computeIfAbsent(host, s -> idCount.incrementAndGet());
                    edgeList.add(new Edge(hostIDs.get(requestedHost), hostIDs.get(host), value));
                }
                for (Map.Entry<String, Integer> entry : hostIDs.entrySet()) {
                    hostList.add(new Host(entry.getKey(), entry.getValue()));
                }

                map.put("nodes",hostList);
                map.put("edges",edgeList);
            }
        }

        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(map);
        resp.setContentType("application/json");
        resp.setContentLength(json.getBytes().length);
        resp.getWriter().print(json);


    }

    public static class Edge {
        private final String arrows = "to";
        private int from, to, value;

        public Edge(int from, int to, int value) {
            this.from = from;
            this.to = to;
            this.value = value;
        }

        public String getArrows() {
            return arrows;
        }

        public int getFrom() {
            return from;
        }

        public int getTo() {
            return to;
        }

        public String  getLabel() {
            return String.valueOf(value);
        }
    }

    public static class Host {
        private String label;
        private int id;

        public Host(String label, int id) {
            this.label = label;
            this.id = id;
        }

        public String getLabel() {
            return label;
        }

        public int getId() {
            return id;
        }
    }
}
