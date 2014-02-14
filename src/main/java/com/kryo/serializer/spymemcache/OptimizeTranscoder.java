package com.kryo.serializer.spymemcache;



import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.kryo.serializer.kryo.KryoFactory;

import net.spy.memcached.CachedData;
import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.transcoders.Transcoder;
import net.spy.memcached.transcoders.TranscoderUtils;
import net.spy.memcached.util.StringUtils;

public class OptimizeTranscoder extends SpyObject implements Transcoder<Object> {

	static final Logger LOGGER = LogManager.getLogger(OptimizeTranscoder.class);

	static final int SPECIAL_BYTE = 1;
	static final int SPECIAL_BOOLEAN = 8192;
	static final int SPECIAL_INT = 4;
	static final int SPECIAL_LONG = 16384;
	static final int SPECIAL_CHARACTER = 16;
	static final int SPECIAL_STRING = 32;
	static final int SPECIAL_STRINGBUFFER = 64;
	static final int SPECIAL_FLOAT = 128;
	static final int SPECIAL_SHORT = 256;
	static final int SPECIAL_DOUBLE = 512;
	static final int SPECIAL_DATE = 1024;
	static final int SPECIAL_STRINGBUILDER = 2048;
	static final int SPECIAL_BYTEARRAY = 4096;
	static final int GZIP = 2;
	static final int SERIALIZED = 8;
	static final int KRYO = 32768;
	static final int SNAPPY = 65536;

	static final int GZIP_THRESHOLD = 30720;
	static final int SNAPPY_THRESHOLD = 1024;


	static final String DEFAULT_CHARSET = "UTF-8";
	protected String charset = DEFAULT_CHARSET;

	private int maxSize;

	static final boolean PACK_ZEROS = false;
	private final TranscoderUtils tu = new TranscoderUtils(PACK_ZEROS);

	private boolean kryoEnabled = false;
	private boolean snappyEnabled = false;

	private static ThreadLocal<Kryo> kryo = new ThreadLocal<Kryo>() {
		@Override
		protected Kryo initialValue() {
			return KryoFactory.getInstance();
		}
	};

	public OptimizeTranscoder() {
		super();
		this.maxSize = CachedData.MAX_SIZE;
	}

	public void setKryoEnabled(boolean kryoEnabled) {
		this.kryoEnabled = kryoEnabled;
		this.snappyEnabled = kryoEnabled;
	}

	@Override
	public Object decode(CachedData d) {
		byte[] data = d.getData();
		Object rv = null;
		if (data != null) {

			if ((d.getFlags() & KRYO) != 0) {
				ByteArrayInputStream bais = null;
				SnappyInputStream sis = null;
				Input input = null;
				try {
					InputStream is = bais = new ByteArrayInputStream(data);
					if ((d.getFlags() & SNAPPY) != 0) {
						is = sis = new SnappyInputStream(bais);
					}
					input = new Input(is);
					rv = kryo.get().readClassAndObject(input);
				} catch (InvalidClassException ice) {
					LOGGER.warn("Stored and local class incompatible : " + ice.getMessage());
				} catch (Exception ioe) {
					LOGGER.error("IOException during KRYO deserialization", ioe);
				} finally {
					if (input != null) {
						input.close();
					}
					if (sis != null) {
						try {
							sis.close();
						} catch (IOException e) {
							LOGGER.info("JAVA Serializer error : close ObjectOutputStream", e);
						}
					}
					if (bais != null) {
						try {
							bais.close();
						} catch (IOException e) {
						}
					}
				}

			} else if ((d.getFlags() & SERIALIZED) != 0) {
				ByteArrayInputStream bais = null;
				GZIPInputStream gzis = null;
				ObjectInputStream ois = null;
				try {
					InputStream is = bais = new ByteArrayInputStream(data);
					if ((d.getFlags() & GZIP) != 0) {
						is = gzis = new GZIPInputStream(bais);
					}
	                ois = new ObjectInputStream(is) {
	                    @Override
	                    protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
	                        try {
	                            //When class is not found,try to load it from context class loader.
	                            return super.resolveClass(desc);
	                        } catch (ClassNotFoundException e) {
	                            return Thread.currentThread().getContextClassLoader().loadClass(desc.getName());
	                        }
	                    }
	                };
	                rv = ois.readObject();
				} catch (Exception ice) {
					LOGGER.warn("Stored and local class incompatible : " + ice.getMessage());
				} finally {
					if (ois != null) {
						try {
							ois.close();
						} catch (IOException e) {}
					}
					if (gzis != null) {
						try {
							gzis.close();
						} catch (IOException e) {}
					}
					if (bais != null) {
						try {
							bais.close();
						} catch (IOException e) {}
					}
				}

			} else if ((d.getFlags() & GZIP) != 0) {
				data = gzipDecompress(d.getData());

			} else if ((d.getFlags() & SNAPPY) != 0) {
				data = snappyDecompress(d.getData());
			}

			if ((d.getFlags() & (KRYO | SERIALIZED)) == 0) {
				int f = d.getFlags() & ~ (GZIP | SNAPPY);
				switch(f) {
				case SPECIAL_BOOLEAN:
					rv = Boolean.valueOf(decodeBoolean(data));
					break;
				case SPECIAL_INT:
					rv = Integer.valueOf(tu.decodeInt(data));
					break;
				case SPECIAL_SHORT:
					rv = Short.valueOf((short) tu.decodeInt(data));
					break;
				case SPECIAL_LONG:
					rv = Long.valueOf(tu.decodeLong(data));
					break;
				case SPECIAL_DATE:
					rv = new Date(tu.decodeLong(data));
					break;
				case SPECIAL_BYTE:
					rv = Byte.valueOf(tu.decodeByte(data));
					break;
				case SPECIAL_FLOAT:
					rv = new Float(Float.intBitsToFloat(tu.decodeInt(data)));
					break;
				case SPECIAL_DOUBLE:
					rv = new Double(Double.longBitsToDouble(tu.decodeLong(data)));
					break;
				case SPECIAL_BYTEARRAY:
					rv = data;
					break;
				case SPECIAL_STRING:
					rv = decodeString(data);
					break;
				case SPECIAL_STRINGBUFFER:
					rv = new StringBuffer(decodeString(data));
					break;
				case SPECIAL_STRINGBUILDER:
					rv = new StringBuilder(decodeString(data));
					break;
				case SPECIAL_CHARACTER:
					rv = decodeCharacter(data);
					break;
				}
			}
		}
		return rv;
	}

	@Override
	public CachedData encode(Object o) {
		byte[] b = null;
		int flags = 0;
		if (o instanceof String) {
			b = encodeString((String) o);
			flags |= SPECIAL_STRING;
			if (StringUtils.isJsonObject((String) o)) {
				return new CachedData(flags, b, getMaxSize());
			}
		} else if (o instanceof StringBuffer) {
			flags |= SPECIAL_STRINGBUFFER;
			b = encodeString(String.valueOf(o));
		} else if (o instanceof StringBuilder) {
			flags |= SPECIAL_STRINGBUILDER;
			b = encodeString(String.valueOf(o));
		} else if (o instanceof Long) {
			b = tu.encodeLong((Long) o);
			flags |= SPECIAL_LONG;
		} else if (o instanceof Integer) {
			b = tu.encodeInt((Integer) o);
			flags |= SPECIAL_INT;
		} else if (o instanceof Short) {
			b = tu.encodeInt((Short) o);
			flags |= SPECIAL_SHORT;
		} else if (o instanceof Boolean) {
			b = encodeBoolean((Boolean) o);
			flags |= SPECIAL_BOOLEAN;
		} else if (o instanceof Date) {
			b = tu.encodeLong(((Date) o).getTime());
			flags |= SPECIAL_DATE;
		} else if (o instanceof Byte) {
			b = tu.encodeByte((Byte) o);
			flags |= SPECIAL_BYTE;
		} else if (o instanceof Float) {
			b = tu.encodeInt(Float.floatToIntBits((Float) o));
			flags |= SPECIAL_FLOAT;
		} else if (o instanceof Double) {
			b = tu.encodeLong(Double.doubleToLongBits((Double) o));
			flags |= SPECIAL_DOUBLE;
		} else if (o instanceof byte[]) {
			b = (byte[]) o;
			flags |= SPECIAL_BYTEARRAY;
		} else if (o instanceof Character) {
			b = tu.encodeInt((Character) o);
			flags |= SPECIAL_CHARACTER;

		} else if (kryoEnabled) {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			Output output = new Output(bos);
			kryo.get().writeClassAndObject(output, o);
			output.flush();
			b = bos.toByteArray();
			output.close();
			flags |= KRYO;

		} else {
			b = serialize(o);
			flags |= SERIALIZED;
		}

		assert b != null;

		if (snappyEnabled) {
			if (b.length > SNAPPY_THRESHOLD) {
				byte[] compressed = snappyCompress(b);
				if (compressed != null && compressed.length < b.length) {
					b = compressed;
					flags |= SNAPPY;
				}
			}

		} else {
			if (b.length > GZIP_THRESHOLD) {
				byte[] compressed = gzipCompress(b);
				if (compressed != null && compressed.length < b.length) {
					b = compressed;
					flags |= GZIP;
				} 
			}
		}

		return new CachedData(flags, b, getMaxSize());
	}

	/**
	 * Decode the string with the current character set.
	 */
	protected String decodeString(byte[] data) {
		String rv = null;
		try {
			if (data != null) {
				rv = new String(data, charset);
			}
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
		return rv;
	}

	/**
	 * Encode a string into the current character set.
	 */
	protected byte[] encodeString(String in) {
		byte[] rv = null;
		try {
			rv = in.getBytes(charset);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
		return rv;
	}

	protected Character decodeCharacter(byte[] b) {
		return Character.valueOf((char) tu.decodeInt(b));
	}

	public byte[] encodeBoolean(boolean b) {
		byte[] rv = new byte[1];
		rv[0] = (byte) (b ? 1 : 0);
		return rv;
	}

	public boolean decodeBoolean(byte[] in) {
		assert in.length == 1 : "Wrong length for a boolean";
		return in[0] == 1;
	}

	@Override
	public boolean asyncDecode(CachedData d) {
		return false;
	}

	@Override
	public int getMaxSize() {
		return maxSize;
	}

	/**
	 * Get the bytes representing the given serialized object.
	 */
	protected byte[] serialize(Object o) {
		if (o == null) {
			throw new NullPointerException("SPYMemcached JAVA Serialization error : Can't serialize null");
		}
		byte[] rv = null;
		ByteArrayOutputStream bos = null;
		ObjectOutputStream os = null;
		try {
			bos = new ByteArrayOutputStream();
			os = new ObjectOutputStream(bos);
			os.writeObject(o);
			os.close();
			bos.close();
			rv = bos.toByteArray();
		} catch (IOException e) {
			throw new IllegalArgumentException("SPYMemcached JAVA Serialization error : Non-serializable object", e);
		} finally {
			if (os != null) {
				try {
					os.close();
				} catch (IOException e) {
					LOGGER.info("JAVA Serializer error : close ObjectOutputStream", e);
				}
			}
			if (bos != null) {
				try {
					bos.close();
				} catch (IOException e) {
					LOGGER.info("JAVA Serializer error : close ByteArrayOutputStream", e);
				}
			}
		}
		return rv;
	}

	private byte[] gzipCompress(byte[] in) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		GZIPOutputStream gzip = null;
		try {
			gzip = new GZIPOutputStream(baos);
			gzip.write(in);
		} catch (IOException e) {
			LOGGER.warn("GZIP compression failed", e);
			return null;
		} finally {
			if (gzip != null) {
				try {
					gzip.close();
				} catch (IOException e) {
					LOGGER.info("GZIP compression error : close GZIPOutputStream", e);
				}
			}
			if (baos != null) {
				try {
					baos.close();
				} catch (IOException e) {
					LOGGER.info("GZIP compression error : close ByteArrayOutputStream", e);
				}
			}
		}
		return baos.toByteArray();
	}

	private byte[] gzipDecompress(byte[] in) {
		ByteArrayOutputStream baos = null;
		if (in != null) {
			ByteArrayInputStream bais = new ByteArrayInputStream(in);
			baos = new ByteArrayOutputStream();
			GZIPInputStream gzip = null;
			try {
				gzip = new GZIPInputStream(bais);
				byte[] buf = new byte[16 * 1024];
				int r = -1;
				while ((r = gzip.read(buf)) > 0) {
					baos.write(buf, 0, r);
				}
			} catch (IOException e) {
				LOGGER.error("GZIP decompression failed", e);
				baos = null;
			} finally {
				if (gzip != null) {
					try {
						gzip.close();
					} catch (IOException e) {
						LOGGER.info("GZIP compression error : close GZIPInputStream", e);
					}
				}
				if (bais != null) {
					try {
						bais.close();
					} catch (IOException e) {
						LOGGER.info("GZIP compression error : close ByteArrayInputStream", e);
					}
				}
			}
		}
		return baos == null ? null : baos.toByteArray();
	}

	private byte[] snappyCompress(byte[] in) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(in.length);
		SnappyOutputStream snappy = null;
		try {
			snappy = new SnappyOutputStream(baos);
			snappy.write(in);
		} catch (IOException e) {
			LOGGER.warn("SNAPPY compression failed", e);
			return null;
		} finally {
			if (snappy != null) {
				try {
					snappy.close();
				} catch (IOException e) {
					LOGGER.info("SNAPPY compression error : close SnappyOutputStream", e);
				}
			}
			try {
				baos.close();
			} catch (IOException e) {
				LOGGER.info("SNAPPY compression error : close ByteArrayOutputStream", e);
			}
		}
		return baos.toByteArray();
	}

	private byte[] snappyDecompress(byte[] in) {
		ByteArrayOutputStream baos = null;
		if (in != null) {
			ByteArrayInputStream bais = new ByteArrayInputStream(in);
			baos = new ByteArrayOutputStream();
			SnappyInputStream snappy = null;
			try {
				snappy = new SnappyInputStream(bais);
				byte[] buf = new byte[16 * 1024];
				int r = -1;
				while ((r = snappy.read(buf)) > 0) {
					baos.write(buf, 0, r);
				}
			} catch (IOException e) {
				LOGGER.error("SNAPPY decompression failed", e);
				baos = null;
			} finally {
				if (snappy != null) {
					try {
						snappy.close();
					} catch (IOException e) {
						LOGGER.info("SNAPPY compression error : close SnappyInputStream", e);
					}
				}
				if (bais != null) {
					try {
						bais.close();
					} catch (IOException e) {
						LOGGER.info("SNAPPY compression error : close ByteArrayInputStream", e);
					}
				}
			}
		}
		return baos == null ? null : baos.toByteArray();
	}
}
