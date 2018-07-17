package com.yc.hadoop.reduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import com.yc.hadoop.bean.SaleKeyWritable;
import com.yc.hadoop.bean.SaleWritable;

public class SaleReduce extends Reducer<SaleKeyWritable, SaleWritable, SaleKeyWritable, SaleWritable>{
	@Override
	protected void reduce(SaleKeyWritable key, Iterable<SaleWritable> values, Context context) throws IOException, InterruptedException {
		double total = 0;
		for (SaleWritable sw : values) {
			total += sw.getPrice();
		}
		SaleWritable sw = new SaleWritable();
		sw.setPrice(total);
		// 输出
		context.write(key, sw);
	}
}
