package com.kryo.serializer.kryo;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import org.junit.Test;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.kryo.serializer.bean.BeanSerializable;
import com.kryo.serializer.spymemcache.SpyMemcached;

public class MemcacheOptimPrefTest extends AbstractBenchmark {
	
	private static SpyMemcached spyMemcachedKryo = new SpyMemcached("localhost:11211", true);
	private static SpyMemcached spyMemcachedClassic = new SpyMemcached("localhost:11211", false);
	private final static BeanSerializable bs = getBean();
	
	@Test
	@BenchmarkOptions(benchmarkRounds = 100000)
	public void serialization_memcache_classic() throws ClassNotFoundException, IOException {

		spyMemcachedClassic.put(bs, "CLASSIC", 14400);
		BeanSerializable bsRead = (BeanSerializable) spyMemcachedClassic.get("CLASSIC");
		
		assertEquals('C', bsRead.getMyChar());
	}
	
	@Test
	@BenchmarkOptions(benchmarkRounds = 100000)
	public void serialization_memcache_kryo() throws ClassNotFoundException, IOException {

		spyMemcachedKryo.put(bs, "KRYO", 14400);
		BeanSerializable bsRead = (BeanSerializable) spyMemcachedKryo.get("KRYO");
		
		assertEquals('C', bsRead.getMyChar());
	}

	private static BeanSerializable getBean() {
		BeanSerializable bs = new BeanSerializable();
		bs.setMyInteger(42);
		bs.setMyBoolean(true);
		bs.setMyByte(Byte.parseByte("1"));
		bs.setMyChar('C');
		bs.setMyDate(new Date(1392116197393L));
		bs.setMyDouble(123);
		bs.setMyFloat(2);
		ArrayList<String> arrayList = new ArrayList<>();
		arrayList.add("David");
		bs.setMyList(arrayList);
		return bs;
	}
}
