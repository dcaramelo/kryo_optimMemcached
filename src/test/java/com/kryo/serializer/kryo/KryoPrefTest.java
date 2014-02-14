package com.kryo.serializer.kryo;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Date;

import org.junit.Test;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.kryo.serializer.bean.BeanSerializable;

public class KryoPrefTest extends AbstractBenchmark {
	
	private final static BeanSerializable bs = getBean();

	@Test
	@BenchmarkOptions(benchmarkRounds = 100000)
	public void serialization_classic() throws ClassNotFoundException, IOException {

		ObjectOutputStream oos;
		ByteArrayOutputStream byteArrayOS = null;
		try {
			byteArrayOS = new ByteArrayOutputStream();
			oos = new ObjectOutputStream(byteArrayOS);
			oos.writeObject(bs);
			oos.flush();
			oos.close();
		} catch (java.io.IOException e) {
			e.printStackTrace();
		}
		
		byte[] byteArray = byteArrayOS.toByteArray();
		
		ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(byteArray));
		BeanSerializable bsRead = (BeanSerializable) ois.readObject();
		
		assertEquals('C', bsRead.getMyChar());
	}
	
	@Test
	@BenchmarkOptions(benchmarkRounds = 100000)
	public void serialization_kryo() throws ClassNotFoundException, IOException {

		Kryo instance = KryoFactory.getInstance();
		Output output = new Output(4096);
		instance.writeClassAndObject(output, bs);
		output.flush();

		byte[] bytes = output.toBytes();
		Input input = new Input(bytes);
		BeanSerializable bsRead = (BeanSerializable) instance.readClassAndObject(input);
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
