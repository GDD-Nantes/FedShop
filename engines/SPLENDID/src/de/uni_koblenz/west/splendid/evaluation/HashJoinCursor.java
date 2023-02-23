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
package de.uni_koblenz.west.splendid.evaluation;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.LookAheadIteration;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;

/**
 * Hash join on two result sets.
 * First the bindings of the left join argument are put in a hash table.
 * Then the bindings of the right argument are matched.
 * 
 * @author Olaf Goerlitz
 */
public class HashJoinCursor extends LookAheadIteration<BindingSet, QueryEvaluationException> {
	
	protected final CloseableIteration<BindingSet, QueryEvaluationException> leftIter;
	protected final CloseableIteration<BindingSet, QueryEvaluationException> rightIter;
	protected final List<String> joinBindingNames;
	
	protected Deque<BindingSet> joinedBindings = new ArrayDeque<BindingSet>();
	protected HashMap<List<Binding>, List<BindingSet>> joinHashMap;
	
	private volatile boolean closed;
	
	public HashJoinCursor(CloseableIteration<BindingSet, QueryEvaluationException> leftIter, CloseableIteration<BindingSet, QueryEvaluationException> rightIter, Set<String> joinVars)
		throws QueryEvaluationException {

		this.leftIter = leftIter;
		this.rightIter = rightIter;
		this.joinBindingNames = new ArrayList<String>(joinVars);
	}
	
	private void buildHashMap() {
		
		try {
			this.joinHashMap = new HashMap<List<Binding>, List<BindingSet>>();
			
			// populate hash map with left side results
			while (!closed && leftIter.hasNext()) {
				BindingSet next = leftIter.next();
				
				// compile join bindings of current binding set
				// (cross product will result in empty bindings list)
				List<Binding> joinBindings = new ArrayList<Binding>();
				for (String bindingName : this.joinBindingNames) {
					joinBindings.add(next.getBinding(bindingName));
				}

				// add join bindings to hash map
				List<BindingSet> bindings = joinHashMap.get(joinBindings);
				if (bindings == null) {
					bindings = new ArrayList<BindingSet>();
					joinHashMap.put(joinBindings, bindings);
				}
				bindings.add(next);
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Stop the evaluation and close any open cursor.
	 */
	@Override
	protected void handleClose() throws QueryEvaluationException {
		closed = true;

		// close left side cursor
		leftIter.close();
		rightIter.close();
	}

	@Override
	protected BindingSet getNextElement() throws QueryEvaluationException {
		
		if (joinHashMap == null)
			buildHashMap();
		
		// return next joined binding if available
		if (joinedBindings.size() != 0)
			return joinedBindings.remove();
		
		if (!rightIter.hasNext())
			return null;
		
		// or generate next join bindings
		// get next original binding set until join partner is found
		List<BindingSet> bindings = null;
		BindingSet next = null;

		while (bindings == null) {

			if (!rightIter.hasNext())
				return null;

			next = rightIter.next();
			
			// compile join bindings of current binding set
			// (cross product will result in empty bindings list)
			List<Binding> joinBindings = new ArrayList<Binding>();
			for (String bindingName : this.joinBindingNames) {
				joinBindings.add(next.getBinding(bindingName));
			}
			
			bindings = joinHashMap.get(joinBindings);
		}			
		
		// create all join combinations
		for (BindingSet binding : bindings) {
			QueryBindingSet set = new QueryBindingSet(next);
			set.addAll(binding);
			joinedBindings.add(set);
		}
		
		return joinedBindings.remove();
	}

}
