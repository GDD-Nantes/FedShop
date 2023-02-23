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
package de.uni_koblenz.west.splendid.config;

import static org.eclipse.rdf4j.sail.config.SailConfigSchema.SAILTYPE;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.ValueFactoryImpl;
import org.eclipse.rdf4j.sail.config.SailConfigException;
import org.eclipse.rdf4j.sail.config.SailImplConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic configuration object for managing sail configuration settings
 * of a certain type. In contrast to SailImplConfigBase, which only supports
 * sail:sailType, it can be used for configuration options of different types. 
 * 
 * @author Olaf Goerlitz
 */
public abstract class AbstractSailConfig implements SailImplConfig {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSailConfig.class);
	
	private String type;
	private IRI typePredicate;
	
	protected AbstractSailConfig() {
		this.typePredicate = SAILTYPE;
	}
	
	protected AbstractSailConfig(IRI typePredicate) {
		this.typePredicate = typePredicate;
	}
	
	@Override
	public String getType() {
		return type;
	}

	// Dummy getter
	@Override
	public long getIterationCacheSyncThreshold() {
		return Long.MAX_VALUE;
	}
	
	protected void setType(String type) {
		this.type = type;
	}

	@Override
	public Resource export(Model model) {
		ValueFactoryImpl vf = ValueFactoryImpl.getInstance();

		BNode implNode = vf.createBNode();

		if (type != null) {
			model.add(implNode, this.typePredicate, vf.createLiteral(type));
		}

		return implNode;
	}

	@Override
	public void parse(Model model, Resource implNode) throws SailConfigException {
		Literal typeLit = getObjectLiteral(model, implNode, this.typePredicate);
		if (typeLit != null) {
			this.type = typeLit.getLabel();
		}
	}

	@Override
	public void validate() throws SailConfigException {
		if (type == null) {
			throw new SailConfigException("No implementation type specified: use " + this.typePredicate);
		}
	}
	
	// -------------------------------------------------------------------------
	
	/**
	 * Returns the literal value of the triple's object matching the predicate.
	 * 
	 * @param model the model of the configuration settings.
	 * @param implNode the model representing a configuration setting.
	 * @param predicate the predicate defining a configuration attribute.
	 * @return the literal value of the object or null.
	 * @throws SailConfigException if there is no literal to return.
	 */
	protected Literal getObjectLiteral(Model model, Resource implNode, IRI property) throws SailConfigException {
		Iterator<Statement> objects = model.filter(implNode, property, null).iterator();
		if (!objects.hasNext())
			return null;
		Statement st = objects.next();
		if (objects.hasNext())
			throw new SailConfigException("found multiple object values for " + property);
		Value object = st.getObject();
		if (object instanceof Literal)
			return (Literal) object;
		else
			throw new SailConfigException("object value is not a Literal: " + property + " " + object); 
	}
	
	/**
	 * Returns the boolean value of the triple's object matching the predicate.
	 * 
	 * @param model the model of the configuration settings.
	 * @param implNode the model representing a configuration setting.
	 * @param predicate the predicate defining a configuration attribute.
	 * @return the boolean value of the object or the default value.
	 * @throws SailConfigException if there is no (single) resource to return.
	 */
	protected boolean getObjectBoolean(Model model, Resource implNode, IRI property, boolean defaultValue) throws SailConfigException {
		try {
			return getObjectLiteral(model, implNode, property).booleanValue();
		} catch (NullPointerException e) {
			LOGGER.trace("missing option " + property + ", default is " + defaultValue);
			return defaultValue;
		} catch (IllegalArgumentException e) {
			throw new SailConfigException("not a boolean value in option " + property);
		}
	}
	
	/**
	 * Returns the object resource of the triple matching the supplied predicate.
	 * 
	 * @param model the model of the configuration settings.
	 * @param implNode the model representing a configuration setting.
	 * @param predicate the predicate defining a configuration attribute.
	 * @return the resource representing the configuration attribute or null.
	 * @throws SailConfigException if there is no (single) resource to return.
	 */
	protected Resource getObjectResource(Model model, Resource implNode, IRI predicate) throws SailConfigException {
		Iterator<Statement> objects = model.filter(implNode, predicate, null).iterator();
		if (!objects.hasNext())
			return null;
//			throw new SailConfigException("found no object value for " + predicate);
		Statement st = objects.next();
		if (objects.hasNext())
			throw new SailConfigException("found multiple object values for " + predicate);
		Value object = st.getObject();
		if (object instanceof Resource)
			return (Resource) object;
		else
			throw new SailConfigException("object value is not a Resource: " + predicate + " " + object); 
	}
	
	/**
	 * Helper method to extract a configuration's sub setting.
	 * 
	 * @param model the configuration model
	 * @param implNode node representing a specific configuration context.
	 * @param option configuration option to look for
	 * @return set of found values for the configuration setting.
	 */
//	protected Set<Value> filter(Model model, Resource implNode, IRI option) { // Sesame 3
	protected Set<Value> filter(Model model, Resource implNode, IRI option) { // Sesame 2
//		return model.filter(implNode, MEMBER, null).objects(); // Sesame 3
		// Sesame 2:
		Set<Value> values = new HashSet<Value>();
		Iterator<Statement> objects = model.filter(implNode, option, null).iterator();
		while (objects.hasNext()) {
			values.add(objects.next().getObject());
		}
		return values;
	}

}
