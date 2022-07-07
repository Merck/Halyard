package com.msd.gin.halyard.common;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

public final class SSLSettings {
	public String keyStoreLocation;
	public String keyStoreType = "jks";
	public char[] keyStorePassword;
	public String trustStoreLocation;
	public char[] trustStorePassword;
	public String sslProtocol = "TLS";

	public SSLContext createSSLContext() throws IOException, GeneralSecurityException {
		KeyManager[] keyManagers = null;
		if (keyStoreLocation != null && !keyStoreLocation.isEmpty()) {
			KeyStore keyStore = KeyStore.getInstance(keyStoreType);
			try (InputStream keyStoreIn = new URL(keyStoreLocation).openStream()) {
				keyStore.load(keyStoreIn, (keyStorePassword != null && keyStorePassword.length > 0) ? keyStorePassword : null);
			}
			KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
			keyManagerFactory.init(keyStore, keyStorePassword);
			keyManagers = keyManagerFactory.getKeyManagers();
		}
	
		TrustManager[] trustManagers = null;
		if (trustStoreLocation != null && !trustStoreLocation.isEmpty()) {
			KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
			try (InputStream trustStoreIn = new URL(trustStoreLocation).openStream()) {
				trustStore.load(trustStoreIn, (trustStorePassword != null && trustStorePassword.length > 0) ? trustStorePassword : null);
			}
			TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
			trustManagerFactory.init(trustStore);
			trustManagers = trustManagerFactory.getTrustManagers();
		}
	
		SSLContext sslContext = SSLContext.getInstance(sslProtocol);
		sslContext.init(keyManagers, trustManagers, null);
		return sslContext;
	}

}
