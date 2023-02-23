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
package de.uni_koblenz.west.splendid.optimizer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.helpers.VarNameCollector;

/**
 * @author Olaf Goerlitz
 */
public class PatternSelectivityOptimizer extends AbstractFederationOptimizer {
	
	@Override
	public TupleExpr optimizeBGP(TupleExpr bgp) {
		
		List<TupleExpr> queryExpressions = this.getBaseExpressions(bgp);
		final Map<TupleExpr, Double> costs = new HashMap<TupleExpr, Double>();
		
		// get cardinality for all query base expressions
		for (TupleExpr expr : queryExpressions) {
			costs.put(expr, this.costEstimator.process(expr));
		}
		
		// defines the cost comparator
		Comparator<TupleExpr> comparator = new Comparator<TupleExpr>() {
			@Override
			public int compare(TupleExpr arg0, TupleExpr arg1) {
				return costs.get(arg0).compareTo(costs.get(arg1));
			}
		};
		
		// create new query by adding expressions with minimum cost first and
		// trying to avoid cross products
		TupleExpr newQuery = null;
		while (!queryExpressions.isEmpty()) {
			
			List<TupleExpr> joinCandidates = new ArrayList<TupleExpr>();
			
			// find join candidates (unless it is a new query)
			if (newQuery != null) {
				// collect all query expressions containing a join variable
				// (to avoid cross products)
				Set<String> queryVars = VarNameCollector.process(newQuery);
				for (TupleExpr expr : queryExpressions) {
					for (String varName : VarNameCollector.process(expr)) {
						if (queryVars.contains(varName)) {
							joinCandidates.add(expr);
							break;
						}
					}
				}
			}
			
			// no candidates if this is a new query or in case of cross product
			if (joinCandidates.size() == 0)
				joinCandidates.addAll(queryExpressions);
			
			// sort expressions by cost
			Collections.sort(joinCandidates, comparator);
			
			// join with selected expression and remove from list
			TupleExpr joinExpr = joinCandidates.get(0);
			newQuery = (newQuery == null) ? joinExpr : new Join(newQuery, joinExpr);
			queryExpressions.remove(joinExpr);
		}
		
		bgp.replaceWith(newQuery);
		return newQuery;
	}

}
