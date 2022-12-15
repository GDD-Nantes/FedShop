package org.example;

import org.eclipse.rdf4j.federated.FedXConfig;
import org.eclipse.rdf4j.federated.FedXFactory;
import org.eclipse.rdf4j.federated.algebra.StatementSource;
import org.eclipse.rdf4j.federated.repository.FedXRepository;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVWriter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Federapp {
    private static final Logger log = LoggerFactory.getLogger(Federapp.class);
    public static final Map<String, Object> CONTAINER = new ConcurrentHashMap<>();
    public static final String SOURCE_SELECTION_KEY = "SOURCE_SELECTION";
    public static final String SOURCE_SELECTION2_KEY = "SOURCE_SELECTION_DO_SOURCE_SELECTION";
    public static final String COUNT_HTTP_REQ_KEY = "HTTPCOUNTER";
    public static final String LIST_HTTP_REQ_KEY = "HTTPLIST";

    public static final String MAP_SS = "MAP_SS";

    private static int sumDistinctSourceSelection() throws Exception {
        Map<StatementPattern, List<StatementSource>> stmt = ((Map<StatementPattern, List<StatementSource>>) Federapp.CONTAINER
                .get(Federapp.SOURCE_SELECTION2_KEY));
        int counter = 0;
        Set<StatementSource> set = new HashSet<>();
        for (StatementPattern pattern : stmt.keySet()) {
            for (StatementSource source : stmt.get(pattern)) {
                if (set.add(source)) {
                    counter++;
                }

            }
        }
        return counter;
    }

    private static int sumSourceSelection() throws Exception {
        Map<StatementPattern, List<StatementSource>> stmt = ((Map<StatementPattern, List<StatementSource>>) Federapp.CONTAINER
                .get(Federapp.SOURCE_SELECTION2_KEY));
        int counter = 0;
        for (StatementPattern pattern : stmt.keySet()) {
            for (StatementSource source : stmt.get(pattern)) {
                counter++;
            }
        }
        return counter;
    }

    private static void createSourceSelectionFile(String sourceSelectionPath) throws Exception {
        BufferedWriter sourceSelectionWriter = new BufferedWriter(new FileWriter(sourceSelectionPath));
        sourceSelectionWriter.write("triple,source_selection\n");
        Map<StatementPattern, List<StatementSource>> stmt = ((Map<StatementPattern, List<StatementSource>>) Federapp.CONTAINER
                .get(Federapp.SOURCE_SELECTION2_KEY));
        if (stmt != null) {
            for (StatementPattern pattern : stmt.keySet()) {
                sourceSelectionWriter.write(
                        ("\"" + pattern + "\"," + "\"" + stmt.get(pattern).toString()).replace("\n", " ") + "\"\n");
            }
        }
        sourceSelectionWriter.close();
    }

    private static void createResultFile(String resultFilePath, List<BindingSet> results) throws Exception {
        BufferedWriter queryResultWriter = new BufferedWriter(new FileWriter(resultFilePath));
        System.out.println("SAVING RESULT");

        for (BindingSet b : results) {
            System.out.println(b.toString());
            queryResultWriter.write(b.toString() + "\n");
        }

        System.out.println("SAVED RESULT");
        queryResultWriter.close();
    }

    private static void createHttpListFile(String path) throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(path));

        Queue<String> q = (Queue) CONTAINER.get(LIST_HTTP_REQ_KEY);
        for (String s : q) {
            writer.write(s + "\n");
        }

        writer.close();
    }

    private static void parseSS(String path) throws Exception {
        /*
         * Parse a provenance table
         * Map each tp to a set of non-null sources
         */
        String rawSS = new String(Files.readAllBytes(Paths.get(path)));
        String[] tabSS = rawSS.split("\n");
        Map<Integer, Set<String>> tpMap = new ConcurrentHashMap<>();

        int lineId = 0;
        for (String line : tabSS) {
            if (lineId > 0) {
                int tpId = 0;
                for (String source : line.split(",")) {
                    if (!tpMap.containsKey(tpId)) {
                        tpMap.put(tpId, new HashSet<>());
                    }
                    String ss = source.replace("http://", "");
                    if (!ss.isEmpty()) {
                        // www.shop69.fr -> sparql_www.shop69.fr
                        tpMap.get(tpId).add("sparql_" + ss);
                    }
                    tpId++;
                }
            }
            lineId++;
        }

        CONTAINER.put(MAP_SS, tpMap);
    }

    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        // init
        CONTAINER.put(COUNT_HTTP_REQ_KEY, new AtomicInteger());
        CONTAINER.put(LIST_HTTP_REQ_KEY, new ConcurrentLinkedQueue<>());
        String configPath = args[0];
        String queryPath = args[1];
        String resultPath = args[2];
        String statPath = args[3];
        String sourceSelectionPath = args[4];
        String httpListFilePath = args[5];

        if (args.length > 6) {
            String ssPath = args[6];
            parseSS(ssPath);
        }

        CSVWriter statWriter = new CSVWriter(
            new FileWriter(statPath), ',', 
            CSVWriter.NO_QUOTE_CHARACTER, 
            CSVWriter.DEFAULT_ESCAPE_CHARACTER, 
            CSVWriter.DEFAULT_LINE_END
        );

        String rawQuery = new String(Files.readAllBytes(Paths.get(queryPath)));
        log.info("Query {}", rawQuery);
        File dataConfig = new File(configPath);

        Long startTime = null;
        Long endTime = null;
        AtomicBoolean success = new AtomicBoolean(true);

        FedXRepository repo = FedXFactory.newFederation()
                .withConfig(new FedXConfig()
                        .withEnableMonitoring(true)
                        .withLogQueryPlan(true)
                        .withLogQueries(true)
                        .withDebugQueryPlan(true)
                        .withEnforceMaxQueryTime(86400))
                .withMembers(dataConfig)
                .create();

        startTime = System.currentTimeMillis();

        List<BindingSet> results = new ArrayList<>(10000);
        try (RepositoryConnection conn = repo.getConnection()) {
            TupleQuery tq = conn.prepareTupleQuery(rawQuery);
            try (TupleQueryResult res = tq.evaluate()) {
                while (res.hasNext()) {
                    results.add(res.next());
                }
            }
        }

        endTime = System.currentTimeMillis();
        System.out.println(success.get());

        long durationTime = endTime - startTime;

        // resultPath: .../{engine}/{query}/{instance_id}/batch_{batch_id}/default/results
        // research in reverse order

        Pattern pattern = Pattern.compile(".*/(\\w+)/(q\\d+)/(\\d+)/batch_(\\d+)/(\\w+)/results");
        Matcher basicInfos = pattern.matcher(resultPath);
        String engine = basicInfos.group(1);
        String query = basicInfos.group(2);
        String instance = basicInfos.group(3);
        String batch = basicInfos.group(4);
        String mode = basicInfos.group(5);

        String distinct_ss = "" + sumDistinctSourceSelection();
        String total_ss = "" + sumSourceSelection();

        int httpqueries = ((AtomicInteger) CONTAINER.get(COUNT_HTTP_REQ_KEY)).get();

        String[] header = {"query","engine","instance","batch","mode","exec_time","distinct_ss","nb_http_request","total_ss"};
        statWriter.writeNext(header);

        String[] content = {query, engine, instance, batch, mode, Long.toString(durationTime), distinct_ss, Integer.toString(httpqueries), total_ss};
        statWriter.writeNext(content);
        statWriter.close();

        createResultFile(resultPath, results);
        createSourceSelectionFile(sourceSelectionPath);
        createHttpListFile(httpListFilePath);

        repo.shutDown();
    }
}
