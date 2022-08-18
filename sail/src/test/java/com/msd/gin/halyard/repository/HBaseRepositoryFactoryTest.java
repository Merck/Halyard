package com.msd.gin.halyard.repository;

import org.junit.Test;

import static org.junit.Assert.*;

public class HBaseRepositoryFactoryTest {

    @Test
	public void testGetRepositoryType() {
		assertEquals("openrdf:HBaseRepository", new HBaseRepositoryFactory().getRepositoryType());
    }

    @Test
    public void testGetConfig() {
		assertTrue(new HBaseRepositoryFactory().getConfig() instanceof HBaseRepositoryConfig);
    }
}
