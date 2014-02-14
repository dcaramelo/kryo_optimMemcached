package com.kryo.serializer.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

public class BeanSerializable implements Serializable {

	private static final long serialVersionUID = 12344325L;

	// Primitive
	private int myInteger;
	private boolean myBoolean;
	private long myLong;
	private byte myByte;
	private short myShort;
	private float myFloat;
	private double myDouble;
	private char myChar;
	
	// Object
	private String myString;
	private ArrayList<String> myList;
	private HashMap<Integer, String> myMap;
	private HashSet<Integer> mySet;
	private Date myDate;
	
	public BeanSerializable() {
	}

	public int getMyInteger() {
		return myInteger;
	}
	
	public void setMyInteger(int myInteger) {
		this.myInteger = myInteger;
	}
	public boolean isMyBoolean() {
		return myBoolean;
	}
	public void setMyBoolean(boolean myBoolean) {
		this.myBoolean = myBoolean;
	}
	public long getMyLong() {
		return myLong;
	}
	public void setMyLong(long myLong) {
		this.myLong = myLong;
	}
	public byte getMyByte() {
		return myByte;
	}
	public void setMyByte(byte myByte) {
		this.myByte = myByte;
	}
	public short getMyShort() {
		return myShort;
	}
	public void setMyShort(short myShort) {
		this.myShort = myShort;
	}
	public float getMyFloat() {
		return myFloat;
	}
	public void setMyFloat(float myFloat) {
		this.myFloat = myFloat;
	}
	public double getMyDouble() {
		return myDouble;
	}
	public void setMyDouble(double myDouble) {
		this.myDouble = myDouble;
	}
	public char getMyChar() {
		return myChar;
	}
	public void setMyChar(char myChar) {
		this.myChar = myChar;
	}
	public String getMyString() {
		return myString;
	}
	public void setMyString(String myString) {
		this.myString = myString;
	}
	public ArrayList<String> getMyList() {
		return myList;
	}
	public void setMyList(ArrayList<String> myList) {
		this.myList = myList;
	}
	public HashMap<Integer, String> getMyMap() {
		return myMap;
	}
	public void setMyMap(HashMap<Integer, String> myMap) {
		this.myMap = myMap;
	}
	public HashSet<Integer> getMySet() {
		return mySet;
	}
	public void setMySet(HashSet<Integer> mySet) {
		this.mySet = mySet;
	}
	public Date getMyDate() {
		return myDate;
	}
	public void setMyDate(Date myDate) {
		this.myDate = myDate;
	}
}
