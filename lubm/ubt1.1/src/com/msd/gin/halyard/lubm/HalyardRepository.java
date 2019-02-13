package com.msd.gin.halyard.lubm;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.eclipse.rdf4j.common.iteration.Iterations;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import com.msd.gin.halyard.sail.HBaseSail;

public class HalyardRepository implements edu.lehigh.swat.bench.ubt.api.Repository {

	private String ontology;
	private Repository repo;

	public void open(String tableName) {
		Configuration config = HBaseConfiguration.create();
		HBaseSail sail = new HBaseSail(config, tableName, true, 0, true, 60, null, null);
		repo = new SailRepository(sail);
		repo.initialize();

		if (ontology != null) {
			System.out.println(String.format("Loading ontology: %s", ontology));
			try(RepositoryConnection conn = repo.getConnection()) {
				conn.add(new URL(ontology), null, RDFFormat.RDFXML);
			} catch(IOException ioe) {
				throw new RuntimeException(ioe);
			}
		}
	}

	public void setOntology(String ontology) {
		this.ontology = ontology;
	}

	public void clear() {
		try(RepositoryConnection conn = repo.getConnection()) {
			conn.clear();
		}
	}

	public void close() {
		repo.shutDown();
	}

	public boolean load(String dataPath) {
		ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		File dir = new File(dataPath);
		for(final File f : dir.listFiles()) {
			executor.submit(() -> {
				System.out.println(String.format("[%s] Loading file: %s", Thread.currentThread().getName(), f));
				try(RepositoryConnection conn = repo.getConnection()) {
					conn.add(f, null, RDFFormat.RDFXML);
				}
				return null;
			});
		}
		executor.shutdown();
		try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
		} catch(InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		return true;
	}

	public edu.lehigh.swat.bench.ubt.api.QueryResult issueQuery(edu.lehigh.swat.bench.ubt.api.Query query) {
		String sparql = query.getString();
		System.out.println(String.format("Executing query:\n%s", sparql));
		try(RepositoryConnection conn = repo.getConnection()) {
			final List<?> res = Iterations.asList(conn.prepareTupleQuery(QueryLanguage.SPARQL, sparql).evaluate());
			return new edu.lehigh.swat.bench.ubt.api.QueryResult() {
				Iterator<?> iter = res.iterator();

				public long getNum() {
					return res.size();
				}

				public boolean next() {
					boolean hasNext = iter.hasNext();
					if(hasNext) {
						iter.next();
					}
					return hasNext;
				}
			};
		}
	}
}

