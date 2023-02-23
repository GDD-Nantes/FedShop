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

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF;

import de.uni_koblenz.west.splendid.index.Graph;
import de.uni_koblenz.west.splendid.model.MappedStatementPattern;
import de.uni_koblenz.west.splendid.statistics.RDFStatistics;

/**
 * @author Olaf Goerlitz
 */
public class SPLENDIDCardinalityEstimator extends VoidCardinalityEstimator {
	
	boolean distSOPerPred;
	
	public SPLENDIDCardinalityEstimator(RDFStatistics stats, boolean distSOPerPred) {
		super(stats);
		this.distSOPerPred = distSOPerPred;
	}
	
	@Override
	public String getName() {
		return "SPLDCard";
	}
	
	@Override
	protected Number getPatternCard(MappedStatementPattern pattern, Graph source) {
		
		Value sVal = pattern.getSubjectVar().getValue();
		Value pVal = pattern.getPredicateVar().getValue();
		Value oVal = pattern.getObjectVar().getValue();
		
		// check trivial cave that all variables are bound
		if (sVal != null && pVal != null && oVal != null)
			return 1;
		
		// handle rdf:type
		if (RDF.TYPE.equals(pVal) && oVal != null) {
			return stats.getTypeCount(source, oVal.stringValue());
		}
		
		Number resultSize;
		if (pVal == null) {
			resultSize = stats.getTripleCount(source);
		} else {
			resultSize = stats.getPredicateCount(source, pVal.stringValue());
		}
		
		// object is bound
		if (oVal != null) {
			if (distSOPerPred && pVal != null) {
				long distPredObj = stats.getDistinctObjects(source, pVal.stringValue());
				if (distPredObj == -1)
					throw new IllegalArgumentException("no value for distinct Objects per Predicate in statistics");
				return resultSize.doubleValue() / distPredObj;
			} else {
				long pCount = stats.getDistinctPredicates(source);
				long distObj = stats.getDistinctObjects(source);
				return resultSize.doubleValue() * pCount / distObj; 
			}
		}
		
		// subject is bound
		if (sVal != null) {
			if (distSOPerPred && pVal != null) {
				long distPredSubj = stats.getDistinctSubjects(source, pVal.stringValue());
				if (distPredSubj == -1)
					throw new IllegalArgumentException("no value for distinct Objects per Predicate in statistics");
				return resultSize.doubleValue() / distPredSubj;
			} else {
				long pCount = stats.getDistinctPredicates(source);
				long distSubj = stats.getDistinctSubjects(source);
				return resultSize.doubleValue() * pCount / distSubj; 
			}
			
		}

		// use triple count containing the predicate
		return resultSize;
	}

}
