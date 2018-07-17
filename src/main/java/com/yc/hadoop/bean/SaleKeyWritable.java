package com.yc.hadoop.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 自定义的key类类型应该实现WritableComparable接口
 */
public class SaleKeyWritable implements WritableComparable<SaleKeyWritable>{
	private String name;
	private String date;
	
	@Override
	public String toString() {
		return "SaleKeyWritable [name=" + name + ", date=" + date + "]";
	}
	
	public SaleKeyWritable() {
		super();
	}

	public SaleKeyWritable(String name, String date) {
		super();
		this.name = name;
		this.date = date;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((date == null) ? 0 : date.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SaleKeyWritable other = (SaleKeyWritable) obj;
		if (date == null) {
			if (other.date != null)
				return false;
		} else if (!date.equals(other.date))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
		this.date = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeUTF(date);
	}

	@Override
	public int compareTo(SaleKeyWritable o) {
		if (o.equals(this)) {
			return 0;
		} else if (o.date.hashCode() > this.date.hashCode()) {
			return 1;
		} else {
			return -1;
		}
	}
}
