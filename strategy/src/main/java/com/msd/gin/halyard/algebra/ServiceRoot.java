/**
 * Copyright (c) 2016 Eclipse RDF4J contributors.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */
package com.msd.gin.halyard.algebra;

import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

public final class ServiceRoot extends QueryRoot {
    private static final long serialVersionUID = 7052207623408379003L;

    public static ServiceRoot create(Service service) {
        return new ServiceRoot(service.getArg());
    }

    public final TupleExpr originalServiceArgs;

    private ServiceRoot(TupleExpr serviceArgs) {
        super(serviceArgs.clone());
        this.originalServiceArgs = serviceArgs;
    }
}