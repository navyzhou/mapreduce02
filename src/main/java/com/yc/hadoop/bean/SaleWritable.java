package com.yc.hadoop.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * hadoop自定义类型
 * 自定义类型需要实现WritableComparable接口，如果需要排序
 * 如果不需要排序则可实现Writable
 * @company 源辰信息
 * @author navy
 */
public class SaleWritable implements Writable{
	// 因为姓名已经在key里面了，所有这里就不需要了
	private Double price; // 金额
	
	@Override
	public String toString() {
		return "SaleInfo [price=" + price + "]";
	}

	public Double getPrice() {
		return price;
	}

	public void setPrice(Double price) {
		this.price = price;
	}

	/**
	 * 怎么读取这些属性
	 * 注意：读取的顺序、类型、数量必须跟写入时的一样
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		price = in.readDouble();
	}

	/**
	 * 就是要将这个类的哪些属性，要实现序列化
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(price);
	}
}
