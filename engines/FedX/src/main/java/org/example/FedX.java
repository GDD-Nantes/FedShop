package org.example;

import org.eclipse.rdf4j.federated.FedXConfig;
import org.eclipse.rdf4j.federated.FedXFactory;
import org.eclipse.rdf4j.federated.algebra.StatementSource;
import org.eclipse.rdf4j.federated.repository.FedXRepository;
import org.eclipse.rdf4j.federated.structures.FedXTupleQuery;
import org.eclipse.rdf4j.federated.monitoring.MonitoringUtil;
import org.eclipse.rdf4j.federated.monitoring.QueryLog;
import org.eclipse.rdf4j.federated.monitoring.QueryPlanLog;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.explanation.Explanation;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
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

public class FedX {
    // private static final Logger log = LoggerFactory.getLogger(FedX.class);
    public static final Map<String, Object> CONTAINER = new ConcurrentHashMap<>();
    public static final String SOURCE_SELECTION_KEY = "SOURCE_SELECTION";
    public static final String SOURCE_SELECTION2_KEY = "SOURCE_SELECTION_DO_SOURCE_SELECTION";
    public static final String COUNT_HTTP_REQ_KEY = "HTTPCOUNTER";
    public static final String LIST_HTTP_REQ_KEY = "HTTPLIST";

    public static final String MAP_SS = "MAP_SS";

    private static void createSourceSelectionFile(String sourceSelectionPath) throws Exception {
        try (BufferedWriter sourceSelectionWriter = new BufferedWriter(new FileWriter(sourceSelectionPath))) {
            sourceSelectionWriter.write("triple,source_selection\n");
            Map<StatementPattern, List<StatementSource>> stmt = ((Map<StatementPattern, List<StatementSource>>) FedX.CONTAINER
                    .get(FedX.SOURCE_SELECTION2_KEY));
            if (stmt != null) {
                // String jsonString = new Gson().toJson(stmt);
                // sourceSelectionWriter.write(jsonString);
                for (StatementPattern pattern : stmt.keySet()) {
                    sourceSelectionWriter.write(
                            ("\"" + pattern + "\"," + "\"" + stmt.get(pattern).toString()).replace("\n", " ") + "\"\n");
                }
            }
        }
    }

    private static void parseSourceSelection(String path) throws Exception {
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
                    String ss = source.replace("http://", "").replace("/", "");
                    if (!ss.isEmpty()) {
                        // www.shop69.fr -> sparql_www.shop69.fr
                        tpMap.get(tpId).add("sparql_" + ss + "_");
                    }
                    tpId++;
                }
            }
            lineId++;
        }

        CONTAINER.put(MAP_SS, tpMap);
    }

    public static void main(String[] args) throws Exception {
        Logger log = LoggerFactory.getLogger(FedX.class);
        log.info("Numbers of arguments: " + args.length);
        // init
        CONTAINER.put(COUNT_HTTP_REQ_KEY, new AtomicInteger());
        CONTAINER.put(LIST_HTTP_REQ_KEY, new ConcurrentLinkedQueue<>());
        String configPath = args[0];
        String queryPath = args[1];
        String outResultPath = args[2];
        String outSourceSelectionPath = args[3];
        String outQueryPlanFile = args[4];
        String statPath = args[5];
        String inSourceSelectionPath = "";

        if (args.length == 7) {
            inSourceSelectionPath = args[6];
            parseSourceSelection(inSourceSelectionPath);
        }

        String rawQuery = new String(Files.readAllBytes(Paths.get(queryPath)));
        log.info("Query {}", rawQuery);
        File dataConfig = new File(configPath);

        Long startTime = null;
        Long endTime = null;

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

        try (RepositoryConnection conn = repo.getConnection()) {
            TupleQuery tq = conn.prepareTupleQuery(rawQuery);
            try (TupleQueryResult res = tq.evaluate()) {
                log.info("# Optimized Query Plan: ");
                String queryPlan = QueryPlanLog.getQueryPlan();
                // log.info(queryPlan);

                try (BufferedWriter queryPlanWriter = new BufferedWriter(new FileWriter(outQueryPlanFile))) {
                    if (!outQueryPlanFile.equals("/dev/null")) {
                        queryPlanWriter.write(queryPlan);
                    }
                }

                try (BufferedWriter queryResultWriter = new BufferedWriter(new FileWriter(outResultPath))) {

                    // The execution of hasNext() yield null exception error
                    // What is actually null ? Candidates from debugger: conn (unprobable)

                    while (res.hasNext()) {
                        BindingSet b = res.next();

                        if (!outResultPath.equals("/dev/null")) {
                            queryResultWriter.write(b.toString() + "\n");
                            // String jsonString = new Gson().toJson(b);
                            // queryResultWriter.write(jsonString);
                        }
                    }
                }
            }

            MonitoringUtil.printMonitoringInformation(repo.getFederationContext());
        }

        endTime = System.currentTimeMillis();

        long durationTime = endTime - startTime;

        if (!statPath.equals("/dev/null")) {
            File statFile = new File(statPath);
            if (statFile.getParentFile() != null) {
                statFile.getParentFile().mkdirs();
            }
            statFile.createNewFile();

            CSVWriter statWriter = new CSVWriter(
                    new FileWriter(statFile), ',',
                    CSVWriter.NO_QUOTE_CHARACTER,
                    CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                    CSVWriter.DEFAULT_LINE_END);

            // resultPath:
            // .../{engine}/{query}/{instance_id}/batch_{batch_id}/default/results
            // research in reverse order

            Pattern pattern = Pattern
                    .compile(".*/(\\w+)/(q\\w+)/instance_(\\d+)/batch_(\\d+)/attempt_(\\d+)/stats.csv");
            Matcher basicInfos = pattern.matcher(statPath);
            basicInfos.find();
            String engine = basicInfos.group(1);
            String query = basicInfos.group(2);
            String instance = basicInfos.group(3);
            String batch = basicInfos.group(4);
            String attempt = basicInfos.group(5);

            String[] header = { "query", "engine", "instance", "batch", "attempt", "exec_time" };
            statWriter.writeNext(header);

            String[] content = { query, engine, instance, batch, attempt, Long.toString(durationTime) };
            statWriter.writeNext(content);
            statWriter.close();
        }

        if (!outSourceSelectionPath.equals("/dev/null")) {
            createSourceSelectionFile(outSourceSelectionPath);
        }

        repo.shutDown();

    }
}
