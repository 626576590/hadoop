package com.test.mapreduce.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UserWritable implements WritableComparable<UserWritable> {
	
	private Integer id;
	private Integer income;
	private Integer expenses;
	private Integer sum;
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeInt(income);
		out.writeInt(expenses);
		out.writeInt( sum);
	}

	public void readFields(DataInput in) throws IOException {
		this.id = in.readInt();
		this.income = in.readInt();
		this.expenses = in.readInt();
		this. sum = in.readInt();
	}

	public int compareTo(UserWritable o) {
		return 0;
	}

	public Integer getId() {
		return id;
	}

	public UserWritable setId(Integer id) {
		this.id = id;
		return this;
	}

	public Integer getIncome() {
		return income;
	}

	public UserWritable setIncome(Integer income) {
		this.income = income;
		return this;
	}

	public Integer getExpenses() {
		return expenses;
	}

	public UserWritable setExpenses(Integer expenses) {
		this.expenses = expenses;
		return this;
	}

	public Integer getSum() {
		return sum;
	}

	public UserWritable setSum(Integer sum) {
		this.sum = sum;
		return this;
	}
	
	public String toString() {
		return "{id:"+id+",income:"+income+",expenses:"+expenses+",sum:"+sum+"}";
	}

}
