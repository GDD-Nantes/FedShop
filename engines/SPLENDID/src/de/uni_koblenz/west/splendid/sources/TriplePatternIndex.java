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
package de.uni_koblenz.west.splendid.sources;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.StatementPattern;

/**
 * Index for triple patterns taken from a SPARQL query. The indexing only
 * considers the bound values (constant terms) of a triple pattern.
 * This allows for aggregating triple patterns with different variables but
 * same constant values, e.g. { ?x owl:sameAs ?y . ?y owl:sameAs ?z }.
 * 
 * @author Olaf Goerlitz
 */
public class TriplePatternIndex {

	private Map<Value, Map<Value, Map<Value, List<StatementPattern>>>> pso
		= new HashMap<Value, Map<Value, Map<Value, List<StatementPattern>>>>();
	
	/**
	 * Creates an index and adds the supplied patterns.
	 * 
	 * @param patterns the patterns to be added.
	 */
	public TriplePatternIndex(Collection<StatementPattern> patterns) {
		this.add(patterns);
	}
	
	/**
	 * Adds the supplied set of patterns to the triple pattern index .
	 *  
	 * @param patterns the patterns to be added.
	 */
	public void add(Collection<StatementPattern> patterns) {
		
		for (StatementPattern pattern : patterns) {
			
			Value s = pattern.getSubjectVar().getValue();
			Value p = pattern.getPredicateVar().getValue();
			Value o = pattern.getObjectVar().getValue();

			// build index map P->S->O->{pattern}
			Map<Value, Map<Value, List<StatementPattern>>> soMap = pso.get(p);
			if (soMap == null) {
				soMap = new HashMap<Value, Map<Value, List<StatementPattern>>>();
				pso.put(p, soMap);
			}
			Map<Value, List<StatementPattern>> oMap = soMap.get(s);
			if (oMap == null) {
				oMap = new HashMap<Value, List<StatementPattern>>();
				soMap.put(s, oMap);
			}
			List<StatementPattern> patternSet = oMap.get(o);
			if (patternSet == null) {
				patternSet = new ArrayList<StatementPattern>();
				oMap.put(o, patternSet);
			}
			patternSet.add(pattern);
		}
	}
	
	/**
	 * Returns a list of distinct pattern groups, i.e. pattern which share
	 * the same constant values.
	 *  
	 * @return list of distinct pattern groups.
	 */
	public List<List<StatementPattern>> getDistinctPatterns() {
		List<List<StatementPattern>> patterns = new ArrayList<List<StatementPattern>>();
		for (Value p : pso.keySet()) {
			Map<Value, Map<Value, List<StatementPattern>>> soMap = pso.get(p);
			for (Value s : soMap.keySet()) {
				Map<Value, List<StatementPattern>> oMap = soMap.get(s);
				for (Value o : oMap.keySet()) {
					patterns.add(oMap.get(o));
				}
			}
		}
		return patterns;
	}
	
	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer();
		for (Value p : pso.keySet()) {
			Map<Value, Map<Value, List<StatementPattern>>> soMap = pso.get(p);
			for (Value s : soMap.keySet()) {
				Map<Value, List<StatementPattern>> oMap = soMap.get(s);
				for (Value o : oMap.keySet()) {
					List<StatementPattern> patternSet = oMap.get(o);
					buf.append("<").append(s).append(", ").append(p)
						.append(", ").append(o).append("> #")
						.append(patternSet.size()).append("\n");
				}
			}
		}
		return buf.toString();
	}

}
