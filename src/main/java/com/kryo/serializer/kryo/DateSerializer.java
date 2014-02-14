package com.kryo.serializer.kryo;

import java.util.Date;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class DateSerializer extends Serializer<Date> {
	
	@Override
	public Date read(Kryo kryo, Input input, Class<Date> type) {
		long time = Long.valueOf(input.readString());
		return new java.util.Date(time);
	}

	@Override
	public void write(Kryo kryo, Output output, Date date) {
		String stringValue = String.valueOf(date.getTime());
		output.writeString(stringValue);
	}
}

