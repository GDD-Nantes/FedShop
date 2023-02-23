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

import static de.uni_koblenz.west.splendid.config.FederationSailSchema.VOID_IRI;
import static de.uni_koblenz.west.splendid.config.VoidRepositorySchema.ENDPOINT;

import java.util.Iterator;

//import org.eclipse.rdf4j.model.Model;
//import org.eclipse.rdf4j.model.util.ModelException;
//import org.eclipse.rdf4j.store.StoreConfigException;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.repository.config.RepositoryImplConfigBase;
import org.eclipse.rdf4j.sail.config.SailConfigException;

/**
 * Configuration details for a void repository.
 * 
 * @author Olaf Goerlitz
 */
public class VoidRepositoryConfig extends RepositoryImplConfigBase {
	
	private IRI voidIRI;
	private IRI endpoint;
	
	/**
	 * Returns the location of the VOID file.
	 * 
	 * @return the location of the VOID file or null if it is not set.
	 */
	public IRI getVoidIRI() {
		return this.voidIRI;
	}
	
	/**
	 * Returns the location of the SPARQL endpoint.
	 * 
	 * @return the location of the SPARQL endpoint or null if it is not set.
	 */
	public IRI getEndpoint() {
		return this.endpoint;
	}

	// -------------------------------------------------------------------------
	
	/**
	 * Adds all Repository configuration settings to a configuration model.
	 * 
	 * @param model the configuration model to be filled.
	 * @return the resource representing this repository configuration.
	 */
	@Override
//	public Resource export(Model model) { // Sesame 3
	public Resource export(Model model) { // Sesame 2
		Resource implNode = super.export(model);

		model.add(implNode, VOID_IRI, this.voidIRI);
		
		if (this.endpoint != null)
			model.add(implNode, ENDPOINT, this.endpoint);
		
		return implNode;
	}

	/**
	 * Parses the configuration model.
	 * 
	 * @param model the configuration model.
	 * @param implNode the resource representing this void repository.
	 */
	@Override
//	public void parse(Model model, Resource implNode) throws StoreConfigException { // Sesame 3
	public void parse(Model model, Resource implNode) throws RepositoryConfigException { // Sesame 2
		super.parse(model, implNode);
		
		this.voidIRI = getObjectIRI(model, implNode, VOID_IRI);
		if (this.voidIRI == null)
//			throw new StoreConfigException("VoidRepository requires: " + VOID_IRI);  // Sesame 3
			throw new RepositoryConfigException("VoidRepository requires: " + VOID_IRI);
		
		this.endpoint = getObjectIRI(model, implNode, ENDPOINT);
		
	}

//	/**
//	 * Validates this configuration. If the configuration is invalid a
//	 * {@link StoreConfigException} is thrown including the reason why the
//	 * configuration is invalid.
//	 * 
//	 * @throws StoreConfigException
//	 *             If the configuration is invalid.
//	 */
//	@Override
//	public void validate() throws StoreConfigException {
//		super.validate();
////		if (this.endpoint == null) {
////			throw new StoreConfigException("No SPARQL endpoint specified");
////		}
//	}
	
	/**
	 * Returns the object IRI of the setting with the specified property.
	 * 
	 * @param config the configuration settings.
	 * @param subject the subject (sub context) of the configuration setting.
	 * @param property the configuration property.
	 * @return the IRI value of the desired property setting or null.
	 * @throws SailConfigException if there is no (single) IRI to return.
	 */
	protected IRI getObjectIRI(Model config, Resource subject, IRI property) throws RepositoryConfigException {
		Iterator<Statement> objects = config.filter(subject, property, null).iterator();
		if (!objects.hasNext())
			return null;
//			throw new RepositoryConfigException("found no settings for property " + property);
		Statement st = objects.next();
		if (objects.hasNext())
			throw new RepositoryConfigException("found multiple settings for property " + property);
		Value object = st.getObject();
		if (object instanceof IRI)
			return (IRI) object;
		else
			throw new RepositoryConfigException("property value is not a IRI: " + property + " " + object); 
	}

}
