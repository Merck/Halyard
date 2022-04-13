package com.msd.gin.halyard.common;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import static org.junit.Assert.*;

public class HashesTest {
	@Test
	public void testHash16() {
		assertEquals(0x0000, Hashes.hash16(new byte[] {0}));
		assertEquals((short)0xAF96, Hashes.hash16(Bytes.toBytes("foo")));
		assertEquals((short)0x93C5, Hashes.hash16(Bytes.toBytes("bar")));
		assertEquals((short)0xB025, Hashes.hash16(Bytes.toBytes("foobar")));
	}
}
