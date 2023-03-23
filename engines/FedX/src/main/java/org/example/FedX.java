package org.example;

import org.eclipse.rdf4j.federated.FedXConfig;
import org.eclipse.rdf4j.federated.FedXFactory;
import org.eclipse.rdf4j.federated.algebra.StatementSource;
import org.eclipse.rdf4j.federated.repository.FedXRepository;
import org.eclipse.rdf4j.federated.monitoring.MonitoringUtil;
import org.eclipse.rdf4j.federated.monitoring.QueryPlanLog;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryInterruptedException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVWriter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.TimeoutException;

public class FedX {
    // private static final Logger log = LoggerFactory.getLogger(FedX.class);
    public static final FedXContainer CONTAINER = new FedXContainer();

    private static void createSourceSelectionFile(String sourceSelectionPath) throws Exception {

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
                for (String source : line.split(",", -1)) {
                    if (!tpMap.containsKey(tpId)) {
                        tpMap.put(tpId, new HashSet<>());
                    }
                    String ss = source.replace("http://", "").replace("/", "");
                    System.out.println(ss);
                    if (!ss.isEmpty()) {
                        // www.shop69.fr -> sparql_www.shop69.fr
                        tpMap.get(tpId).add("sparql_" + ss + "_");
                    }
                    tpId++;
                }
            }
            lineId++;
        }

        CONTAINER.setTriplePatternSources(tpMap);
    }

    public static void main(String[] args) throws Exception {
        boolean isInterrupted = false;
        Logger log = LoggerFactory.getLogger(FedX.class);
        log.info("Numbers of arguments: " + args.length);
        log.info("Numbers of arguments: " + args.length);
        // init
        String configPath = args[0];
        String queryPath = args[1];
        String outResultPath = args[2];
        String outSourceSelectionPath = args[3];
        String outQueryPlanFile = args[4];
        String statPath = args[5];
        Integer timeout = Integer.parseInt(args[6]);
        String inSourceSelectionPath = "";

        if (args.length == 8) {
            inSourceSelectionPath = args[7];
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
                        .withJoinWorkerThreads(30)
                        .withUnionWorkerThreads(30)
                        .withBoundJoinBlockSize(30)
                        .withEnforceMaxQueryTime(timeout))
                .withMembers(dataConfig)
                .create();

        startTime = System.currentTimeMillis();

        // RepositoryConnection conn = repo.getConnection();
        // TupleQuery tq = conn.prepareTupleQuery(rawQuery);
        // TupleQueryResult res = tq.evaluate();

        // while (res.hasNext()) {
        //     BindingSet b = res.next();
        //     System.out.println(b.toString());
        // }

        try (RepositoryConnection conn = repo.getConnection()) {
            TupleQuery tq = conn.prepareTupleQuery(rawQuery);

            log.info("# Optimized Query Plan: ");
            String queryPlan = QueryPlanLog.getQueryPlan();
            // log.info(queryPlan);

            if (!outQueryPlanFile.equals("/dev/null")) {
                try (BufferedWriter queryPlanWriter = new BufferedWriter(new FileWriter(outQueryPlanFile))) {
                    queryPlanWriter.write(queryPlan);
                }
            }

            if (!outSourceSelectionPath.equals("/dev/null")) {
                try (BufferedWriter sourceSelectionWriter = new BufferedWriter(
                        new FileWriter(outSourceSelectionPath))) {
                    sourceSelectionWriter.write("triple,source_selection\n");
                    Map<StatementPattern, List<StatementSource>> stmt = FedX.CONTAINER.getStmtToSources();
                    if (stmt != null) {
                        // String jsonString = new Gson().toJson(stmt);
                        // sourceSelectionWriter.write(jsonString);
                        for (StatementPattern pattern : stmt.keySet()) {
                            sourceSelectionWriter.write(
                                    ("\"" + pattern + "\"," + "\"" + stmt.get(pattern).toString()).replace("\n", " ")
                                            + "\"\n");
                        }
                    }
                }
            }
            
            try (TupleQueryResult res = tq.evaluate()) {
                try (BufferedWriter queryResultWriter = new BufferedWriter(new FileWriter(outResultPath))) {

                    // The execution of hasNext() yield null exception error
                    // What is actually null ? Candidates from debugger: conn (unprobable)

                    while (res.hasNext()) {
                        BindingSet b = res.next();

                        if (!outResultPath.equals("/dev/null")) {
                            queryResultWriter.write(b.toString() + "\n");
                        }
                    }
                }
            } catch (OutOfMemoryError oom) {
                isInterrupted = true;
                writeEmptyStats(statPath, "oom");
            } catch (Exception exception) {
                isInterrupted = true;
                if (exception.getMessage().contains("has run into a timeout")) {
                    writeEmptyStats(statPath, "timeout");
                } else {
                    throw exception;
                }
            }

            MonitoringUtil.printMonitoringInformation(repo.getFederationContext());
        } finally {
            repo.shutDown();
        }

        endTime = System.currentTimeMillis();

        long durationTime = endTime - startTime;
        int nbHttpQueries = CONTAINER.getHttpReqCount().get();
        long sourceSelectionTime = CONTAINER.getSourceSelectionTime();
        long planningTime = CONTAINER.getPlanningTime();

        if (!statPath.equals("/dev/null") && !isInterrupted) {
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

            Pattern pattern = Pattern
                    .compile(".*/(\\w+)/(q\\w+)/instance_(\\d+)/batch_(\\d+)/attempt_(\\d+)/stats.csv");
            Matcher basicInfos = pattern.matcher(statPath);
            basicInfos.find();
            String engine = basicInfos.group(1);
            String query = basicInfos.group(2);
            String instance = basicInfos.group(3);
            String batch = basicInfos.group(4);
            String attempt = basicInfos.group(5);

            String[] header = { 
                "query", "engine", "instance", "batch", "attempt", "exec_time", "http_req",
                "source_selection_time", "planning_time"
            };
            statWriter.writeNext(header);

            String[] content = { 
                query, engine, instance, batch, attempt, Long.toString(durationTime),
                    Integer.toString(nbHttpQueries), Long.toString(sourceSelectionTime), 
                    Long.toString(planningTime)
                };
            statWriter.writeNext(content);
            statWriter.close();
        }
    }

    private static void writeEmptyStats(String statPath, String reason) throws IOException {
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

            Pattern pattern = Pattern
                    .compile(".*/(\\w+)/(q\\w+)/instance_(\\d+)/batch_(\\d+)/attempt_(\\d+)/stats.csv");
            Matcher basicInfos = pattern.matcher(statPath);
            basicInfos.find();
            String engine = basicInfos.group(1);
            String query = basicInfos.group(2);
            String instance = basicInfos.group(3);
            String batch = basicInfos.group(4);
            String attempt = basicInfos.group(5);

            String[] header = { "query", "engine", "instance", "batch", "attempt", "exec_time", "http_req" };
            statWriter.writeNext(header);

            String[] content = { query, engine, instance, batch, attempt, reason, reason };
            statWriter.writeNext(content);
            statWriter.close();
        }
    }
}
