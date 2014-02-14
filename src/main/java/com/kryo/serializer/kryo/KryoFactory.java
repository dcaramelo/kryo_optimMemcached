package com.kryo.serializer.kryo;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.TreeSet;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.shaded.org.objenesis.strategy.StdInstantiatorStrategy;

public class KryoFactory {
	
	private static Kryo kryo;
	
	public KryoFactory() {}
	
	public static synchronized Kryo getInstance() {
		if(kryo == null) {
			kryo = new Kryo();
			kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
			kryo.register(HashSet.class);
			kryo.register(HashMap.class);
			kryo.register(ArrayList.class);
			kryo.register(ArrayDeque.class);
			kryo.register(TreeSet.class);
			kryo.register(TreeMap.class);
			kryo.register(LinkedList.class);
			kryo.register(LinkedHashSet.class);
			kryo.register(LinkedHashMap.class);
			kryo.register(java.util.Date.class, new DateSerializer());
			kryo.register(Collections.EMPTY_LIST.getClass());
			kryo.register(Collections.EMPTY_MAP.getClass());
			kryo.register(Collections.EMPTY_SET.getClass());
		}
		
		return kryo;
	}
}
