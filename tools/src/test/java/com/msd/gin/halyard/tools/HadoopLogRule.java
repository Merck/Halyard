package com.msd.gin.halyard.tools;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class HadoopLogRule implements TestRule {
	public static HadoopLogRule create() {
		return new HadoopLogRule();
	}

	@Override
	public Statement apply(Statement base, Description description) {
		return new Statement() {
			@Override
			public void evaluate() throws Throwable {
				try {
					base.evaluate();
				} catch(Throwable err) {
					Pattern errPattern = Pattern.compile("Exception|Error", Pattern.CASE_INSENSITIVE);
					Path clusterHome = Paths.get("target/testCluster");
					if (Files.exists(clusterHome)) {
						Files.walk(Paths.get("target/testCluster"))
						.filter(p -> "syslog".equals(p.getFileName().toString()))
						.forEach(f -> {
							try(Stream<String> lines = Files.lines(f)) {
								String log = lines.filter(l -> errPattern.matcher(l).matches() || l.startsWith("\tat "))
		    					.map(l -> "*** "+l+"\n")
		    					.reduce("", String::concat);
								System.out.println(log);
			    			} catch(IOException ioe) {
			    				ioe.printStackTrace();
							}
						});
					} else {
						System.out.println("*** It appears the cluster failed to start");
					}
					throw err;
				}
			}
		};
	}
}
