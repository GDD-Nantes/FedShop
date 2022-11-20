package org.example;

import org.eclipse.rdf4j.federated.FedXConfig;
import org.eclipse.rdf4j.federated.FedXFactory;
import org.eclipse.rdf4j.federated.algebra.StatementSource;
import org.eclipse.rdf4j.federated.repository.FedXRepository;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Federapp {
    private static final Logger log = LoggerFactory.getLogger(Federapp.class);
    public static final Map<String,Object> CONTAINER = new ConcurrentHashMap<>();
    public static final String SOURCE_SELECTION_KEY = "SOURCE_SELECTION";
    public static final String SOURCE_SELECTION2_KEY = "SOURCE_SELECTION_DO_SOURCE_SELECTION";
    public static final String COUNT_HTTP_REQ_KEY = "HTTPCOUNTER";
    public static final String LIST_HTTP_REQ_KEY = "HTTPLIST";

    public static final String MAP_SS = "MAP_SS";


    public static final String CSV_HEADER = "query,exec_time,total_distinct_ss,nb_http_request,total_ss\n";

    private static List<BindingSet> evaluate(RepositoryConnection conn, String rawQuery) throws Exception {
        TupleQuery tq = conn.prepareTupleQuery(rawQuery);
        List<BindingSet> results = new ArrayList<>(10000);
        try  {
            TupleQueryResult tqRes = tq.evaluate();
            for (BindingSet b : tqRes) {
                results.add(b);
            }
        }catch (Exception e) {
            throw new Exception(e);
        }
        return results;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        // init
        CONTAINER.put(COUNT_HTTP_REQ_KEY,new AtomicInteger());
        CONTAINER.put(LIST_HTTP_REQ_KEY,new ConcurrentLinkedQueue<>());
        String configPath = args[0];
        String queryPath = args[1];
        String resultPath = args[2];
        String statPath = args[3];
        String sourceSelectionPath = args[4];
        String httpListFilePath = args[5];
        boolean enableTimeout = true;

        if(args.length > 6) {
            String ssPath= args[6];
            parseSS(ssPath);
        }


        BufferedWriter statWriter = new BufferedWriter(new FileWriter(statPath));

        String rawQuery = new String(Files.readAllBytes(Paths.get(queryPath)));
        log.info("Query {}", rawQuery);
        File dataConfig = new File(configPath);

        Long startTime = null;
        Long endTime = null;
        AtomicBoolean success = new AtomicBoolean(true);


        FedXRepository repo  = FedXFactory.newFederation()
                .withMembers(dataConfig)
                .withConfig(new FedXConfig()
                        .withEnableMonitoring(true)
                        .withLogQueryPlan(true)
                        .withLogQueries(true)
                        .withDebugQueryPlan(true)
                        .withEnforceMaxQueryTime(86400)
                )
                .create();

        RepositoryConnection conn = null;
        try {
            conn = repo.getConnection();
        }catch (RepositoryException e) {
            log.error("Error", e);
            throw e;
        }


        startTime = System.currentTimeMillis();


        RepositoryConnection finalConn = conn;
        CompletableFuture<List<BindingSet>> future = CompletableFuture.supplyAsync(() -> {
            try {
                return Federapp.evaluate(finalConn,rawQuery);
            } catch (Exception e) {
                log.error(String.valueOf(e));
                success.set(false);
                return null;
            }
        });

        List<BindingSet> results = null;
        if(enableTimeout) {
            try {
                results =  future.get(15,TimeUnit.MINUTES);
            }catch (TimeoutException e) {
                log.error("Timeout", e);
                success.set(false);
            }
        } else {
            results = future.join();
        }

        endTime = System.currentTimeMillis();


        if(success.get()) {
            createResultFile(resultPath, results);
            long durationTime = endTime - startTime;
            statWriter.write(CSV_HEADER);

            int httpqueries = ((AtomicInteger) CONTAINER.get(COUNT_HTTP_REQ_KEY)).get();
            statWriter.write(
                    queryPath + ","
                            + durationTime + ","
                            + sumDistinctSourceSelection() + ","
                            + httpqueries + ","
                            + sumSourceSelection() +
                            "\n");


            createSourceSelectionFile(sourceSelectionPath);
            createHttpListFile(httpListFilePath);
        } else {
            statWriter.write(CSV_HEADER);
            statWriter.write(queryPath + "," +"failed,failed,failed" + "\n");

            createSourceSelectionFile(sourceSelectionPath);
            createHttpListFile(httpListFilePath);
        }

        repo.shutDown();
        statWriter.close();
    }


    private static int sumDistinctSourceSelection() throws Exception {
        Map<StatementPattern, List<StatementSource>> stmt = ((Map<StatementPattern, List<StatementSource>>)Federapp.CONTAINER.get(Federapp.SOURCE_SELECTION2_KEY));
        int counter = 0;
        Set<StatementSource> set = new HashSet<>();
        for (StatementPattern pattern :
                stmt.keySet()) {
            for (StatementSource source:stmt.get(pattern)) {
                if(set.add(source)) {
                    counter++;
                }

            }
        }
        return counter;
    }

    private static int sumSourceSelection() throws Exception {
        Map<StatementPattern, List<StatementSource>> stmt = ((Map<StatementPattern, List<StatementSource>>)Federapp.CONTAINER.get(Federapp.SOURCE_SELECTION2_KEY));
        int counter = 0;
        for (StatementPattern pattern :
                stmt.keySet()) {
            for (StatementSource source:stmt.get(pattern)) {
                counter++;
            }
        }
        return counter;
    }

    private static void createSourceSelectionFile(String sourceSelectionPath) throws Exception {
        BufferedWriter sourceSelectionWriter = new BufferedWriter(new FileWriter(sourceSelectionPath));
        sourceSelectionWriter.write("triple,source_selection\n");
        Map<StatementPattern, List<StatementSource>> stmt = ((Map<StatementPattern, List<StatementSource>>)Federapp.CONTAINER.get(Federapp.SOURCE_SELECTION2_KEY));
        if(stmt != null) {
            for (StatementPattern pattern: stmt.keySet()) {
                sourceSelectionWriter.write(("\"" + pattern + "\"," + "\"" +stmt.get(pattern).toString()).replace("\n"," ") + "\"\n");
            }
        }
        sourceSelectionWriter.close();
    }

    private static void createResultFile(String resultFilePath, List<BindingSet> results) throws Exception{
        BufferedWriter queryResultWriter = new BufferedWriter(new FileWriter(resultFilePath));
        System.out.println("SAVING RESULT");

        for (BindingSet b :
                results) {
            System.out.println(b.toString());
            queryResultWriter.write(b.toString() + "\n");
        }

        System.out.println("SAVED RESULT");
        queryResultWriter.close();
    }

    private static void createHttpListFile(String path) throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(path));

        Queue<String> q =(Queue) CONTAINER.get(LIST_HTTP_REQ_KEY);
        for (String s: q) {
            writer.write(s + "\n");
        }

        writer.close();
    }

    private static void parseSS(String path) throws Exception {
        String rawSS = new String(Files.readAllBytes(Paths.get(path)));
        String[] tabSS = rawSS.split("\n");
        Map<Integer,Set<String>> tpMap = new ConcurrentHashMap<>();

        int i = 0;
        for (String l : tabSS) {
            if(i>0){
                int j=0;
                for (String tp : l.split(",")) {
                    if(!tpMap.containsKey(j)){
                        tpMap.put(j, new HashSet<>());
                    }
                    String ss = tp.split("g/")[1].replace("\"","").replace("/","_");
                    tpMap.get(j).add("sparql_example.org_"+ss);
                    j++;
                }
            }
            i++;
        }

        CONTAINER.put(MAP_SS, tpMap);
    }
}
