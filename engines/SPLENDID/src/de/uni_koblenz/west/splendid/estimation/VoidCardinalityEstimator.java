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
package de.uni_koblenz.west.splendid.estimation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.algebra.helpers.VarNameCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.uni_koblenz.west.splendid.index.Graph;
import de.uni_koblenz.west.splendid.model.MappedStatementPattern;
import de.uni_koblenz.west.splendid.statistics.RDFStatistics;

/**
 * Cardinality estimation based on Void descriptions.
 * 
 * @author Olaf Goerlitz
 */
public abstract class VoidCardinalityEstimator extends AbstractCardinalityEstimator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(VoidCardinalityEstimator.class);
	
	protected RDFStatistics stats;
	
	// -------------------------------------------------------------------------
	
	protected abstract Number getPatternCard(MappedStatementPattern pattern, Graph source);
	
	// -------------------------------------------------------------------------
	
	public VoidCardinalityEstimator(RDFStatistics stats) {
		if (stats == null)
			throw new IllegalArgumentException("RDF stats must not be NULL.");
		
		this.stats = stats;
	}
	
	public void meet(MappedStatementPattern pattern) throws RuntimeException {
		
		// check cardinality index first
		if (getIndexCard(pattern) != null)
			return;
		
		double card = 0;

		// estimate the cardinality of the pattern for each source and sum up
		// assumes that all result tuple will be distinct
		for (Graph source : pattern.getSources()) {
			card += getPatternCard(pattern, source).doubleValue();
		}
		
		// add cardinality to index
		setIndexCard(pattern, card);
	}
	
	/**
	 * Group a list of triple patterns by subject.
	 * 
	 * @param patterns the patterns to group.
	 * @return a mapping of subject variables to triple patterns.
	 */
	private Map<Var, List<StatementPattern>> groupBySubject(List<StatementPattern> patterns) {
		Map<Var, List<StatementPattern>> patternGroups = new HashMap<Var, List<StatementPattern>>();
		
		for (StatementPattern p : patterns) {
			
			Var subjectVar = p.getSubjectVar();
			List<StatementPattern> pList = patternGroups.get(subjectVar);
			if (pList == null) {
				pList = new ArrayList<StatementPattern>();
				patternGroups.put(subjectVar, pList);
			}
			pList.add(p);
		}
		return patternGroups;
	}
	
	/**
	 * Returns the cardinality of the supplied pattern.
	 * 
	 * @param p the pattern.
	 * @return the cardinality of the pattern.
	 */
	private double getCardinality(StatementPattern p) {
		Double card = getIndexCard(p);
		if (card == null) {
			meet(p);
			card = getIndexCard(p);
		}
		return card;
	}

	/**
	 * Returns the get selectivity of the supplied pattern.
	 * Based on the distinct subjects per source.
	 * 
	 * @param p the pattern.
	 * @return the selectivity of the pattern.
	 */
	private double getSelectivity(StatementPattern p) {
		long distinctSubjects = 0;
		Set<Graph> sources = ((MappedStatementPattern) p).getSources();
		Value pValue = p.getPredicateVar().getValue();
		
		for (Graph source : sources) {
			// TODO: check that predicate value is not null
			if (pValue == null) {
				distinctSubjects += stats.getDistinctSubjects(source);
			} else {
				distinctSubjects += stats.getDistinctSubjects(source, pValue.stringValue());
			}
		}
		return 1.0 / distinctSubjects;
	}
	
	private double getJoinSelectivity(String varName, List<StatementPattern> leftPatterns, List<StatementPattern> rightPatterns) {

		// compute minimum of all selectivity values (left or right)
		double selectivity = 1;

		// get left pattern selectivity (minimum of all selectivity values)
		for (StatementPattern p : leftPatterns) {
			if (VarNameCollector.process(p).contains(varName)) {
				// get left pattern selectivity
				double pSel = getVarSelectivity((MappedStatementPattern) p, varName);
				if (Double.compare(pSel, selectivity) < 0)
					selectivity = pSel;
			}
		}
		
		// get right pattern selectivity (minimum of all selectivity values)
		for (StatementPattern p : leftPatterns) {
			if (VarNameCollector.process(p).contains(varName)) {
				double pSel = getVarSelectivity((MappedStatementPattern) p, varName);
				if (Double.compare(pSel, selectivity) < 0)
					selectivity = pSel;
			}
		}
		
		return selectivity;
	}
	
	private double computeSubjectBasedCardinality(List<StatementPattern> patterns, Map<Var, Double> sVarCardinality, Map<Var, Double> sVarSelectivity) {
		
		Map<Var, List<StatementPattern>> sVarGroups = groupBySubject(patterns);
		for (Var var : sVarGroups.keySet()) {
			
			List<StatementPattern> sVarPatterns = sVarGroups.get(var);
			
			// handle bound subject variables
			if (var.getValue() != null) {
				for (StatementPattern p : sVarPatterns) {
					double cardinality = getCardinality(p);
					double selectivity = getSelectivity(p);
					sVarCardinality.put(var, cardinality);
					sVarSelectivity.put(var, selectivity);
				}
				continue;
			}
			
			// TODO: optimization: handle list with only one element
			
			boolean boundPO = false;
			Double minPOCardinality = Double.POSITIVE_INFINITY;
			Double minPOSelectivity = Double.POSITIVE_INFINITY;
			List<Double> nonPOCardinalities = new ArrayList<Double>();
			List<Double> nonPOSelectivities = new ArrayList<Double>();
			
			// process all pattern with the same subject variable
			// and compute the join cardinality + selectivity
			for (StatementPattern p : sVarPatterns) {
				
				double cardinality = getCardinality(p);
				double selectivity = getSelectivity(p);
				
				// compute minimum cardinality/selectivity only if P+O are bound
				if (p.getPredicateVar().getValue() != null && p.getObjectVar().getValue() != null) {
					if (Double.compare(cardinality, minPOCardinality) < 0)
						minPOCardinality = cardinality;
					if (Double.compare(selectivity, minPOSelectivity) < 0)
						minPOSelectivity = selectivity;
					boundPO = true;
				} else {
					nonPOCardinalities.add(cardinality);
					nonPOSelectivities.add(selectivity);
				}
			}
			
			// add minimum cardinality/selectivity if computed for patterns with bound P+O
			if (boundPO) {
				nonPOCardinalities.add(minPOCardinality);
				nonPOSelectivities.add(minPOSelectivity);
			}
			
			// compute the join cardinality over all patterns with the same subject 
			Collections.sort(nonPOSelectivities);
			double joinCardinality = nonPOCardinalities.get(0);
			for (int i = 1; i < nonPOCardinalities.size(); i++) {
				joinCardinality *= nonPOCardinalities.get(i);
				joinCardinality *= nonPOSelectivities.get(i-1);
			}
			
			// TODO: check for a possible second join variable in the patterns 
			
			// store cardinality for set of patterns with same subject variable
			// store minimum selectivity 
			sVarCardinality.put(var, joinCardinality);
			sVarSelectivity.put(var, nonPOSelectivities.get(0));

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("VAR ?" + var.getName() + " [" + sVarPatterns.size() + "]: " + sVarPatterns);
				LOGGER.debug("VAR ?" + var.getName() + ": card=" + joinCardinality + ", sel=" + nonPOSelectivities.get(0));
			}
		}
		
		// collect join variables for each group
		Map<Var, Set<String>> joinVarMap = new HashMap<Var, Set<String>>();
		
		for (Var var : sVarGroups.keySet()) {
			List<StatementPattern> sVarPatterns = sVarGroups.get(var);
			
			// collect variable names
			Set<String> vars = new HashSet<String>();
			for (StatementPattern p : sVarPatterns) {
				vars.addAll(VarNameCollector.process(p));
			}
			joinVarMap.put(var, vars);
		}
		
		// compute join selectivity for all possible combination of the groups
		List<Double> selectivityList = new ArrayList<Double>();
		List<Var> vars = new ArrayList<Var>(joinVarMap.keySet());
		for (int i = 0; i < vars.size(); i++) {
			for (int j = i+1; j < vars.size(); j++) {
				Set<String> joinVars = new HashSet<String>(joinVarMap.get(vars.get(i)));
				joinVars.retainAll(joinVarMap.get(vars.get(j)));
				
				double selectivity = 1;
				// if not a cross product: selectivity < 1
				if (!joinVars.isEmpty()) {
					// get selectivity for both join arguments
					
					if (joinVars.size() > 1)
						LOGGER.warn("join estimation for multiple vars");
					
					List<StatementPattern> leftPatterns = sVarGroups.get(vars.get(i));
					List<StatementPattern> rightPatterns = sVarGroups.get(vars.get(j));
					
					// compute selectivity for all join variables (usually just one) and take minimum
					for (String varName : joinVars) {
//						LOGGER.warn("JS " + varName + ": " + leftPatterns + " <-> " + rightPatterns);
						
						double jSel = getJoinSelectivity(varName, leftPatterns, rightPatterns);
						if (Double.compare(jSel, selectivity) < 0)
							selectivity = jSel;
					}
				}
				// add selectivity to list of selectivity values
				selectivityList.add(selectivity);
				
//				LOGGER.warn("compare: " + selectivity + " for " + joinVars + " <-- " + joinVarMap.get(vars.get(i)) + " <-> " + joinVarMap.get(vars.get(j)));
			}
		}
		
		// sort selectivity list
		Collections.sort(selectivityList);
		
		double joinCardinality = sVarCardinality.get(vars.get(0));
		for (int i = 1; i < vars.size(); i++) {
			joinCardinality *= sVarCardinality.get(vars.get(i));
			joinCardinality *= sVarSelectivity.get(vars.get(i-1));
		}
		
//		LOGGER.warn("join cardinality: " + joinCardinality);
		
		return joinCardinality;
	}
	
	// -------------------------------------------------------------------------
	
	@Override
	public void meet(StatementPattern pattern) throws RuntimeException {
		if (pattern instanceof MappedStatementPattern)
			meet((MappedStatementPattern) pattern);
		else
			throw new IllegalArgumentException("cannot estimate cardinality for triple pattern without sources: " + pattern);
	}
	
	/**
	 * Computing the cardinality of a join.
	 * It does not depend on the join order of the triple patterns.
	 * First, joins with the same subject variable are specially treated.
	 * Then, object-object joins and subject-objects joins are handled.
	 * 
	 * @param join the joins used for the cardinality computation.
	 */
	@Override
	public void meet(Join join) throws RuntimeException {
		
		// check cardinality index first
		if (getIndexCard(join) != null)
			return;
		
		// collect all statement patterns
		List<StatementPattern> patterns = StatementPatternCollector.process(join);
		
		// ----------------------------------------------------------
		
		Map<Var, Double> sVarCardinality = new HashMap<Var, Double>();
		Map<Var, Double> sVarSelectivity = new HashMap<Var, Double>();
		
		// group by subject variable and compute cardinality/selectivity for each group
		double card = computeSubjectBasedCardinality(patterns, sVarCardinality, sVarSelectivity);
		
		// add cardinality to index
		setIndexCard(join, card);
		
//		// ----------------------------------------------------------
//		
//		// TODO: does the estimated cardinality depend on the current join?
//		//       -> no: different join order should yield same cardinality
//		
//		// estimate cardinality of join arguments first
//		join.getLeftArg().visit(this);
//		join.getRightArg().visit(this);
//		
//		double joinSelectivity = getJoinSelectivity(join.getLeftArg(), join.getRightArg());
//		
//		double leftCard = getIndexCard(join.getLeftArg());
//		double rightCard = getIndexCard(join.getRightArg());
//		double card = joinSelectivity * leftCard * rightCard;
//
//		// add cardinality to index
//		setIndexCard(join, card);
	}
	
	// -------------------------------------------------------------------------
	
	/**
	 * Get the selectivity for a (named) variable in the supplied pattern.
	 * Computes the selectivity as 1/sum(card_i(P)) for all data sources 'i'. 
	 * 
	 * @param pattern the query pattern to process.
	 * @param varName the variable name.
	 * @return the selectivity of the variable.
	 */
	protected double getVarSelectivity(MappedStatementPattern pattern, String varName) {
		long count = 0;
		
		// TODO: this does not look right yet
		
		for (Graph source : pattern.getSources()) {
			
			Long pCount = stats.getDistinctPredicates(source);
			
			if (varName.equals(pattern.getSubjectVar().getName())) {
				count += stats.getDistinctSubjects(source);
//				count += stats.distinctSubjects(source) / pCount.doubleValue();
				continue;
			}
			if (varName.equals(pattern.getPredicateVar().getName())) {
				throw new UnsupportedOperationException("predicate join not supported yet");
			}
			if (varName.equals(pattern.getObjectVar().getName())) {
				count += stats.getDistinctObjects(source);
//				count += stats.distinctObjects(source) / pCount.doubleValue();
				continue;
			}
			throw new IllegalArgumentException("var name not found in pattern");
		}
		return 1.0 / count;
	}
	
	protected double getJoinSelectivity(TupleExpr leftExpr, TupleExpr rightExpr) {
		
		// get join variables
		Set<String> joinVars = VarNameCollector.process(leftExpr);
		joinVars.retainAll(VarNameCollector.process(rightExpr));
		
		if (joinVars.size() == 0) {
			// cross product: selectivity is 1
			return 1.0;
		}
		if (joinVars.size() == 2) {
			// multi-valued join
			LOGGER.warn("join estimation for multiple vars not supported (yet) - using first var only");
		}
		
		// get all patterns which contain the join variables
		// TODO: extend to more than one join variable
		String joinVar = joinVars.iterator().next();
		List<StatementPattern> leftJoinPatterns = new ArrayList<StatementPattern>();
		List<StatementPattern> rightJoinPatterns = new ArrayList<StatementPattern>();
		
		for (StatementPattern pattern : StatementPatternCollector.process(leftExpr)) {
			if (VarNameCollector.process(pattern).contains(joinVar))
				leftJoinPatterns.add(pattern);
		}
		for (StatementPattern pattern : StatementPatternCollector.process(rightExpr)) {
			if (VarNameCollector.process(pattern).contains(joinVar))
				rightJoinPatterns.add(pattern);
		}
		
		// select one pattern from each join argument to define the join condition
		// TODO: analyze structure of join, current approach produces random estimations
		double leftSel = getVarSelectivity((MappedStatementPattern) leftJoinPatterns.get(0), joinVar);
		double rightSel = getVarSelectivity((MappedStatementPattern) rightJoinPatterns.get(0), joinVar);
		return Math.min(leftSel, rightSel);
	}
	
}
