/**
 * Copyright (c) 2016 Eclipse RDF4J contributors.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */
package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.common.SSLSettings;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;

public final class ElasticSettings {
	String protocol;
	String host;
	int port;
	String username;
	String password;
	String indexName;
	SSLSettings sslSettings;

	public String getProtocol() {
		return protocol;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public String getIndexName() {
		return indexName;
	}

	public static ElasticSettings from(URL esIndexUrl) {
		if (esIndexUrl == null) {
			return null;
		}

		ElasticSettings settings = new ElasticSettings();
		settings.protocol = esIndexUrl.getProtocol();
		settings.host = esIndexUrl.getHost();
		settings.port = esIndexUrl.getPort();
		settings.indexName = esIndexUrl.getPath().substring(1);
		String userInfo = esIndexUrl.getUserInfo();
		if (userInfo != null) {
			String[] creds = userInfo.split(":", 1);
			settings.username = creds[0];
			if (creds.length > 1) {
				settings.password = creds[1];
			}
		}
		return settings;
	}

	public static ElasticSettings from(Configuration conf) throws MalformedURLException {
		String esIndexUrl = conf.get(HBaseSail.ELASTIC_INDEX_URL);
		if (esIndexUrl == null) {
			return null;
		}
		ElasticSettings settings = from(new URL(esIndexUrl));
		settings.username = conf.get("es.net.http.auth.user");
		settings.password = conf.get("es.net.http.auth.pass");
		if ("https".equals(settings.protocol)) {
			settings.sslSettings = SSLSettings.from(conf);
		}
		return settings;
	}
}