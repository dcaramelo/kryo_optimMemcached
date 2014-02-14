package com.kryo.serializer.spymemcache;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.ConnectionFactoryBuilder.Protocol;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;

public class SpyMemcached {
	
	static final Logger LOGGER = LogManager.getLogger(OptimizeTranscoder.class);

	private static final int KEY_MAX_SIZE = 250;
	private MemcachedClient client = null;
	private String serverPoolAdresses;
	private boolean kryoEnabled = false;
	private volatile int hits = 0;
	private volatile int misses = 0;
	private volatile boolean initialized = false;

	public SpyMemcached() {}

	public SpyMemcached(String serverPoolAddresses) {
		this.serverPoolAdresses = serverPoolAddresses;
	}

	public SpyMemcached(String serverPoolAddresses, boolean kryoEnabled) {
		this.serverPoolAdresses = serverPoolAddresses;
		this.kryoEnabled = kryoEnabled;
		init();
	}

	protected void init() {
		OptimizeTranscoder transcoder = new OptimizeTranscoder();
		transcoder.setKryoEnabled(kryoEnabled);
		try {
			client = new MemcachedClient(new ConnectionFactoryBuilder()
					.setProtocol(Protocol.TEXT)
					.setTranscoder(transcoder)
					.setFailureMode(FailureMode.Cancel)
					.build(),
					getAddresses(serverPoolAdresses));
		} catch (IOException e) {}
	}

	protected void destroy() {
		if (client != null) {
			client.shutdown();
		}
	}

	public int getHits() {
		return hits;
	}

	public int getMisses() {
		return misses;
	}

	public Object get(String key) {
		if (key == null || key.length() == 0) {
			return null;
		}
		String sanitizedKey = sanitizeKey(key);
		try {
			final Object obj = client.get(sanitizedKey);
			if (obj != null) {
				hits++;
			} else {
				misses++;
			}			
			return obj;
		} catch (Exception e) {}
	
		return null;
	}

	public Map<String, Object> get(String[] keys) {
		if (keys == null || keys.length == 0) {
			return null;
		}
		List<String> sanitizedKeys = sanitizeKeys(keys);

		// Populate all keys with null values for backwards-compatibility with old Memcached client
		final Map<String, Object> map = new HashMap<String, Object>(sanitizedKeys.size());
		for (String key : sanitizedKeys) {
			map.put(key, null);
		}

		try {
			map.putAll(client.getBulk(sanitizedKeys));
		} catch (Exception e) {}
		return map;
	}

	public boolean invalidate(String key) {
		if (key == null) {
			return false;
		}
		String sanitizedKey = sanitizeKey(key);
		try {
			final Future<Boolean> operationDelete = client.delete(sanitizedKey);
			return operationDelete.get();
		} catch (Exception e) {}
		return false;
	}

	public void asyncInvalidate(String key) {
		if (key == null) {
			return;
		}
		String sanitizedKey = sanitizeKey(key);
		client.delete(sanitizedKey);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("SPYMemcached ASYNCDELETE[%s]", key));
		}
	}

	public boolean isInitialized() {
		return initialized;
	}

	public boolean put(Object obj, String key, int exp) {
		if (key == null) {
			return false;
		}
		String sanitizedKey = sanitizeKey(key);
		try {
			final Future<Boolean> operationSet = client.set(sanitizedKey, exp, obj);
			operationSet.get();
		} catch (Exception e) {
				LOGGER.error(String.format("SPYMemcached PUT[%s] Exception : %s", key, e.getMessage()));
		}
		return false;
	}

	public void asyncPut(Object obj, String key, int exp) {
		if (key == null) {
			return;
		}
		String sanitizedKey = sanitizeKey(key);
		client.set(sanitizedKey, exp, obj);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("SPYMemcached ASYNCPUT[%s %d]", key, exp));
		}
	}

	public long incr(String key, int delta) {
		return client.incr(key, delta);
	}

	public long getCounter(String key) {
		return client.incr(key, 0);
	}

	private String sanitizeKey(String key) {
		try {
			String sanitizedKey = URLEncoder.encode(key, "UTF-8");
			if (sanitizedKey.length() > KEY_MAX_SIZE) {
				LOGGER.warn(String.format("SPYMemcached sanitizeKey[%s] too long key", key));
				sanitizedKey = sanitizedKey.substring(0, KEY_MAX_SIZE);
			}
			return sanitizedKey;
		} catch (UnsupportedEncodingException uee) {
			return key;
		}
	}

	private List<String> sanitizeKeys(String[] keys) {
		List<String> sanitizedKeys = new ArrayList<String>(keys.length);
		for (String key : keys) {
			sanitizedKeys.add(sanitizeKey(key));
		}
		return sanitizedKeys;
	}

	public boolean set(String key, int exp, Object o) {
		return client.set(key, exp, o).isDone();
	}
		
	private List<InetSocketAddress> getAddresses(String s) {
		if (s == null) {
			throw new NullPointerException("Null host list");
		}
		if (s.trim().equals("")) {
        	throw new IllegalArgumentException("No hosts in list:  ``" + s + "''");
        }
        ArrayList<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();

        for (String hoststuff : s.split(" ")) {
        	int finalColon = hoststuff.lastIndexOf(':');
        	if (finalColon < 1) {
        		throw new IllegalArgumentException("Invalid server ``"
        				+ hoststuff + "'' in list:  " + s);
        	}
        	String hostPart = hoststuff.substring(0, finalColon);
        	String portNum = hoststuff.substring(finalColon + 1);
	        addrs.add(new InetSocketAddress(hostPart, Integer.parseInt(portNum)));
        }
        assert !addrs.isEmpty() : "No addrs found";
        return addrs;
	}
}

