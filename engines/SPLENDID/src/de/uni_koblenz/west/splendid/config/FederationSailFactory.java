/*
 * This file is part of RDF Federator.
 * Copyright 2010 Olaf Goerlitz
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
package de.uni_koblenz.west.splendid.config;

import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.repository.config.RepositoryFactory;
import org.eclipse.rdf4j.repository.config.RepositoryImplConfig;
import org.eclipse.rdf4j.repository.config.RepositoryRegistry;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.config.SailConfigException;
import org.eclipse.rdf4j.sail.config.SailFactory;
import org.eclipse.rdf4j.sail.config.SailImplConfig;

import de.uni_koblenz.west.splendid.FederationSail;
import de.uni_koblenz.west.splendid.estimation.AbstractCardinalityEstimator;
import de.uni_koblenz.west.splendid.estimation.AbstractCostEstimator;
import de.uni_koblenz.west.splendid.estimation.CardinalityCostEstimator;
import de.uni_koblenz.west.splendid.estimation.ModelEvaluator;
import de.uni_koblenz.west.splendid.estimation.SPLENDIDCardinalityEstimator;
import de.uni_koblenz.west.splendid.estimation.SPLENDIDCostEstimator;
import de.uni_koblenz.west.splendid.estimation.TrueCardinalityEstimator;
import de.uni_koblenz.west.splendid.estimation.VoidCardinalityEstimator;
import de.uni_koblenz.west.splendid.model.SubQueryBuilder;
import de.uni_koblenz.west.splendid.optimizer.AbstractFederationOptimizer;
import de.uni_koblenz.west.splendid.optimizer.DynamicProgrammingOptimizer;
import de.uni_koblenz.west.splendid.optimizer.PatternSelectivityOptimizer;
import de.uni_koblenz.west.splendid.sources.AskSelector;
import de.uni_koblenz.west.splendid.sources.IndexAskSelector;
import de.uni_koblenz.west.splendid.sources.IndexSelector;
import de.uni_koblenz.west.splendid.sources.SourceSelector;
import de.uni_koblenz.west.splendid.statistics.VoidStatistics;

/**
 * A {@link SailFactory} that creates {@link FederationSail}s
 * based on the supplied configuration data.
 * 
 * ATTENTION: This factory must be published with full package name in
 *            META-INF/services/org.eclipse.rdf4j.sail.config.SailFactory
 * 
 * @author Olaf Goerlitz
 */
public class FederationSailFactory implements SailFactory {
	
	/**
	 * The type of repositories that are created by this factory.
	 * 
	 * @see SailFactory#getSailType()
	 */
	public static final String SAIL_TYPE = "west:FederationSail";

	/**
	 * Returns the Sail's type: <tt>west:FederationSail</tt>.
	 * 
	 * @return the Sail's type.
	 */
	public String getSailType() {
		return SAIL_TYPE;
	}

	/**
	 * Provides a Sail configuration object for the configuration data.
	 * 
	 * @return a {@link FederationSailConfig}.
	 */
	public SailImplConfig getConfig() {
		return new FederationSailConfig();
	}

	/**
	 * Returns a Sail instance that has been initialized using the supplied
	 * configuration data.
	 * 
	 * @param config
	 *            the Sail configuration.
	 * @return The created (but un-initialized) Sail.
	 * @throws SailConfigException
	 *             If no Sail could be created due to invalid or incomplete
	 *             configuration data.
	 */
	@Override
	public Sail getSail(SailImplConfig config) throws SailConfigException { // Sesame 2

		if (!SAIL_TYPE.equals(config.getType())) {
			throw new SailConfigException("Invalid Sail type: " + config.getType());
		}
		
		RepositoryRegistry registry = RepositoryRegistry.getInstance();
		
		assert config instanceof FederationSailConfig;
		FederationSailConfig cfg = (FederationSailConfig)config;
		FederationSail sail = new FederationSail();
		
		// Create all member repositories
		for (RepositoryImplConfig repConfig : cfg.getMemberConfigs()) {
			RepositoryFactory factory = registry.get(repConfig.getType()).get();
			if (factory == null) {
				throw new SailConfigException("Unsupported repository type: " + repConfig.getType());
			}
			try {
				sail.addMember(factory.getRepository(repConfig));
			} catch (RepositoryConfigException e) {
				throw new SailConfigException("invalid repository configuration: " + e.getMessage(), e);
			}
		}

		// create query optimizer
		QueryOptimizerConfig optConfig = cfg.getOptimizerConfig(); 
		AbstractFederationOptimizer opt = getQueryOptimizer(optConfig);
		sail.setFederationOptimizer(opt);
		
		// create evaluation strategy
		sail.setEvalStrategy(optConfig.getEvalStrategy());
		
		// setup statistics
		boolean voidPlus = true;
		String estType = optConfig.getEstimatorType();
		if ("VOID".equalsIgnoreCase(estType))
			voidPlus = false;
		if ("VOID_PLUS".equalsIgnoreCase(estType))
			voidPlus = true;
		
		VoidStatistics stats = VoidStatistics.getInstance();
		AbstractCardinalityEstimator cardEstim = new SPLENDIDCardinalityEstimator(stats, voidPlus);
		AbstractCostEstimator costEstim = new SPLENDIDCostEstimator();
		costEstim.setCardinalityEstimator(cardEstim);
//		ModelEvaluator modelEval = new TrueCardinalityEstimator(sail.getEvalStrategy());
		
		// Create source selector from configuration settings
		SourceSelector selector = getSourceSelector(cfg.getSelectorConfig());
		sail.setSourceSelector(selector);
		
		opt.setBuilder(new SubQueryBuilder(optConfig));
		opt.setSelector(selector);
		opt.setCostEstimator(costEstim);
//		opt.setModelEvaluator(cardEstim);
		opt.setModelEvaluator(costEstim);
//		opt.setModelEvaluator(modelEval);
		

		return sail;
	}
	
	// --------------------------------------------------------------
	
	/**
	 * Creates a sources selector for the given configuration settings.
	 * 
	 * @param selConf the source selector configuration settings.
	 * @return the created sources selector.
	 * @throws SailConfigException If no source selector could be created due to invalid or incomplete
	 *             configuration data.
	 */
	private SourceSelector getSourceSelector(SourceSelectorConfig selConf) throws SailConfigException {
		String selectorType = selConf.getType();
		
		if ("ASK".equalsIgnoreCase(selectorType))
			return new AskSelector();
		else if ("INDEX".equalsIgnoreCase(selectorType))
			return new IndexSelector(selConf.isUseTypeStats());
		else if ("INDEX_ASK".equalsIgnoreCase(selectorType))
			return new IndexAskSelector(selConf.isUseTypeStats());
		
		throw new SailConfigException("invalid source selector type: " + selectorType);
	}
	
	/**
	 * Creates a query optimizer for the given configuration settings.
	 *  
	 * @param optConf the query optimizer configuration settings.
	 * @return the created query optimizer.
	 * @throws SailConfigException If no query optimizer could be created due to invalid or incomplete
	 *             configuration data.
	 */
	private AbstractFederationOptimizer getQueryOptimizer(QueryOptimizerConfig optConf) throws SailConfigException {
		String optimizerType = optConf.getType();
		
		if ("DYNAMIC_PROGRAMMING".equals(optimizerType))
			return new DynamicProgrammingOptimizer(optConf.isUseHashJoin(), optConf.isUseBindJoin());
		else if ("PATTERN_HEURISTIC".equals(optimizerType))
			return new PatternSelectivityOptimizer();
		
		throw new SailConfigException("invalid query optimizer type: " + optConf.getType());
	}
	
}
