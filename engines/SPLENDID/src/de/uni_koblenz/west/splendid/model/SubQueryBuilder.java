/*
 * This file is part of RDF Federator.
 * Copyright 2011 Olaf Goerlitz
 * 
 * RDF Federator is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * RDF Federator is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with RDF Federator.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * RDF Federator uses libraries from the OpenRDF Sesame Project licensed 
 * under the Aduna BSD-style license. 
 */
package de.uni_koblenz.west.splendid.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.VarNameCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.uni_koblenz.west.splendid.config.QueryOptimizerConfig;
import de.uni_koblenz.west.splendid.helpers.OperatorTreePrinter;
import de.uni_koblenz.west.splendid.index.Graph;

/**
 * Creates sub queries for patterns with assigned data sources. 
 * 
 * @author Olaf Goerlitz
 */
public class SubQueryBuilder {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(SubQueryBuilder.class);
	
	private boolean groupBySameAs;
	private boolean groupBySource;
	
	public SubQueryBuilder(QueryOptimizerConfig config) {
		this.groupBySource = config.isGroupBySource();
		this.groupBySameAs = config.isGroupBySameAs();
	}

	/**
	 * Creates sub queries for all mapped data sources.
	 * 
	 * @param patterns the statement patterns with mapped data sources.
	 * @param conditions the filter conditions to apply.
	 * @return a list of created sub queries.
	 */
	public List<TupleExpr> createSubQueries(List<MappedStatementPattern> patterns, List<ValueExpr> conditions) {
		
		if (patterns == null || patterns.size() == 0)
			throw new IllegalArgumentException("need at least on triple pattern to create sub queries");
		
		List<TupleExpr> subQueries = new ArrayList<TupleExpr>();
		
		// create groups of triple patterns according to the configuration
		List<List<MappedStatementPattern>> groups = getGroups(patterns);
		
		// create remote queries for all triple pattern groups
		// triple patterns in groups should all have the same sources
		for (List<MappedStatementPattern> patternGroup : groups) {
			
			// check that all patterns in a group have the same set of source
			// TODO: needs improvement - not the best way to do it
			Set<Graph> sources = null;
			for (MappedStatementPattern pattern : patternGroup) {
				if (sources == null)
					sources = pattern.getSources();
				else
					if (!sources.equals(pattern.getSources())) {
						LOGGER.warn("Statement Patterns with different sources in group");
						sources.addAll(pattern.getSources());
					}
			}
			
			TupleExpr baseExpr = null;
			
			// create a remote query if the pattern group has a single source
			if (sources.size() == 1) {
				for (MappedStatementPattern pattern : patternGroup) {
					baseExpr = (baseExpr == null) ? pattern : new Join(baseExpr, pattern);
				}
				baseExpr = applyFilters(baseExpr, conditions);
				subQueries.add(new RemoteQuery(baseExpr));
			}
			
			// create individual remote queries if there is more than one source 
			else {
				for (MappedStatementPattern pattern : patternGroup) {
					baseExpr = applyFilters(pattern, conditions);
					subQueries.add(new RemoteQuery(baseExpr));
				}
			}
		}
		
		return subQueries;
	}
	
	private TupleExpr applyFilters(TupleExpr expr, List<ValueExpr> conditions) {
		Set<String> varNames = VarNameCollector.process(expr);
		for (ValueExpr condition : conditions) {
			if (varNames.containsAll(VarNameCollector.process(condition))) {
				expr = new Filter(expr, condition);
			}
		}
		return expr;
	}
	
	public List<List<MappedStatementPattern>> getGroups(List<MappedStatementPattern> patterns) {
		
		// sameAs grouping example:
		// ?city :population ?x  -> [A]
		// ?city :founded_in ?y  -> [B]
		// ?city owl:sameAs  ?z  -> [A,C,D]  ... group with first pattern only
		
		// 1. no grouping -> put each single pattern in a set
		// 2. group by source -> put pattern in set based on same source
		// 3. group by sameAs -> put sameAs pattern in set with matched pattern
		// 4. group by source and by sameAs
		
		List<MappedStatementPattern> sameAsPatterns = new ArrayList<MappedStatementPattern>();
		List<List<MappedStatementPattern>> patternGroups = new ArrayList<List<MappedStatementPattern>>();
		
		// start with grouping of sameAs patterns if applicable
		// this removes all sameAs patterns and matched pattern from the list
		if (groupBySameAs) {
			
			// move sameAs patterns from pattern list to sameAs pattern list
			Iterator<MappedStatementPattern> it = patterns.iterator();
			while (it.hasNext()) {
				MappedStatementPattern pattern = it.next();
				if (OWL.SAMEAS.equals(pattern.getPredicateVar().getValue()) && !pattern.getSubjectVar().hasValue()) {
					sameAsPatterns.add(pattern);
					it.remove();
				}
			}
			
			Set<MappedStatementPattern> matchedPatterns = new HashSet<MappedStatementPattern>();
			
			// find all matching pattern for each sameAs pattern
			for (MappedStatementPattern sameAsPattern : sameAsPatterns) {
				
				List<MappedStatementPattern> matchCandidates = new ArrayList<MappedStatementPattern>();
				Set<Graph> sameAsSources = sameAsPattern.getSources();
				Var sameAsSubjectVar = sameAsPattern.getSubjectVar();
				
				// find match candidates
				for (MappedStatementPattern pattern : patterns) {
					Set<Graph> patternSources = pattern.getSources();
					// check if pattern meets condition
					if (containsVar(pattern, sameAsSubjectVar) && sameAsSources.containsAll(patternSources)) {
						matchCandidates.add(pattern);
					}
				}
				
				// check if any match candidate was found
				if (matchCandidates.size() == 0) {
					// found no patterns to match with sameAs
					// add sameAs pattern as its own group
					List<MappedStatementPattern> group = new ArrayList<MappedStatementPattern>();
					group.add(sameAsPattern);
					patternGroups.add(group);
					continue;
				}
				
				// group match candidates with sameAs pattern
				for (MappedStatementPattern pattern : matchCandidates) {
					matchedPatterns.add(pattern);
					// add sameAs pattern with the matched pattern's Sources
					List<MappedStatementPattern> group = new ArrayList<MappedStatementPattern>();
					group.add(pattern);
					group.add(new MappedStatementPattern(sameAsPattern, pattern.getSources()));
					patternGroups.add(group);
				}
			}
			// removed all matched patterns from the pattern list
			patterns.removeAll(matchedPatterns);
		}
		
		if (groupBySource) {
			
			// create map for {Source}->{Pattern}
			Map<Set<Graph>, List<MappedStatementPattern>> sourceMap = new HashMap<Set<Graph>, List<MappedStatementPattern>>();
			
			// add all sameAs groups first, if applicable
			if (groupBySameAs) {
				for (List<MappedStatementPattern> patternList : patternGroups) {
					Set<Graph> sources = patternList.get(0).getSources();
					List<MappedStatementPattern> pList = sourceMap.get(sources);
					if (pList == null) {
						pList = new ArrayList<MappedStatementPattern>();
						sourceMap.put(sources, pList);
					}
					pList.addAll(patternList);
				}
			}
			
			// add all pattern from the list to the source mapping
			for (MappedStatementPattern pattern : patterns) {
				Set<Graph> sources = pattern.getSources();
				List<MappedStatementPattern> pList = sourceMap.get(sources);
				if (pList == null) {
					pList = new ArrayList<MappedStatementPattern>();
					sourceMap.put(sources, pList);
				}
				pList.add(pattern);
			}
			
			// finally create pattern groups for all source mappings
			patternGroups.clear();
//			patternGroups.addAll(sourceMap.values());
			
			// keep groups only if there is no more than one source assigned
			for (Set<Graph> graphs : sourceMap.keySet()) {
				List<MappedStatementPattern> pGroup = sourceMap.get(graphs);
				if (graphs.size() == 1) {
					patternGroups.add(pGroup);
				} else {
					for (MappedStatementPattern pattern : pGroup) {
						List<MappedStatementPattern> pList = new ArrayList<MappedStatementPattern>();
						pList.add(pattern);
						patternGroups.add(pList);
					}
				}
			}
			
		} else {
			
			// add all patterns from the list as individual group
			for (MappedStatementPattern pattern : patterns) {
				List<MappedStatementPattern> pList = new ArrayList<MappedStatementPattern>();
				pList.add(pattern);
				patternGroups.add(pList);
			}
			
		}
		
		// debugging
		for (List<MappedStatementPattern> pList : patternGroups) {
			StringBuffer buffer = new StringBuffer("Group [");
			Set<Graph> sources = null;
			for (MappedStatementPattern pattern : pList) {
				buffer.append(OperatorTreePrinter.print(pattern)).append(", ");
				if (sources == null) {
					sources = pattern.getSources();
				} else {
					if (!sources.equals(pattern.getSources()))
						LOGGER.warn("not the same sources: " + sources + " <-> " + pattern.getSources());
				}
			}
			buffer.setLength(buffer.length()-2);
			buffer.append("] @" + sources);
			
			if (LOGGER.isDebugEnabled())
				LOGGER.debug(buffer.toString());
		}
		
		return patternGroups;
	}

	private boolean containsVar(StatementPattern pattern, Var var) {
		String varName = var.getName();
		Var sVar = pattern.getSubjectVar();
		Var oVar = pattern.getObjectVar();
		return (!sVar.hasValue() && sVar.getName().equals(varName))
			|| (!oVar.hasValue() && oVar.getName().equals(varName));
	}

}
