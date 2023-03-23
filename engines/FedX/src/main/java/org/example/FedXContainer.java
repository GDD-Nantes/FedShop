package org.example;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.rdf4j.federated.algebra.StatementSource;
import org.eclipse.rdf4j.federated.optimizer.SourceSelection;
import org.eclipse.rdf4j.query.algebra.StatementPattern;

public class FedXContainer {

    private Map<StatementPattern, List<StatementSource>> stmtToSources;
    private SourceSelection sourceSelection;
    private Map<Integer, Set<String>> triplePatternSources;
    private AtomicInteger httpReqCount;
    private ConcurrentLinkedQueue<String> httpReqList;
    private long sourceSelectionTime;
    private long planningTime;

    public FedXContainer(){
        this.stmtToSources = new ConcurrentHashMap<>();
        this.sourceSelection = null;
        this.triplePatternSources = new ConcurrentHashMap<>();
        this.httpReqCount = new AtomicInteger();
        this.httpReqList = new ConcurrentLinkedQueue<>();
        this.sourceSelectionTime = 0;
    }

    public Map<StatementPattern, List<StatementSource>> getStmtToSources() {
        return stmtToSources;
    }

    public SourceSelection getSourceSelection() {
        return this.sourceSelection;
    }

    public Map<Integer, Set<String>> getTriplePatternSources() {
        return this.triplePatternSources;
    }

    public AtomicInteger getHttpReqCount() {
        return this.httpReqCount;
    }

    public ConcurrentLinkedQueue<String> getHttpReqList() {
        return this.httpReqList;
    }

    public void setSourceSelection(SourceSelection sourceSelection) {
        this.sourceSelection = sourceSelection;
    }

    public void setStmtToSources(Map<StatementPattern, List<StatementSource>> stmtToSources) {
        this.stmtToSources = stmtToSources;
    }

    public void setTriplePatternSources(Map<Integer, Set<String>> triplePatternSources) {
        this.triplePatternSources = triplePatternSources;
    }

    public void setSourceSelectionTime(long sourceSelectionTime) {
        this.sourceSelectionTime = sourceSelectionTime;
    }

    public long getSourceSelectionTime() {
        return this.sourceSelectionTime;
    }

    public void setPlanningTime(long planningTime) {
        this.planningTime = planningTime;
    }

    public long getPlanningTime() {
        return this.planningTime;
    }


}
