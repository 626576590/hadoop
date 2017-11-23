package com.test.mapreduce.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class WeatherWritable  implements WritableComparable<WeatherWritable>{
	
	private Integer year;
	private Integer month;
	private Integer day;
	private Double temperature;
	
	public Integer getYear() {
		return year;
	}

	public WeatherWritable setYear(Integer year) {
		this.year = year;
		return this;
	}

	public Integer getMonth() {
		return month;
	}

	public WeatherWritable setMonth(Integer month) {
		this.month = month;
		return this;
	}

	public Integer getDay() {
		return day;
	}

	public WeatherWritable setDay(Integer day) {
		this.day = day;
		return this;
	}

	public Double getTemperature() {
		return temperature;
	}

	public WeatherWritable setTemperature(Double temperature) {
		this.temperature = temperature;
		return this;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.year);
		out.writeInt(this.month);
		out.writeInt(this.day);
		out.writeDouble(this.temperature);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
		this.month = in.readInt();
		this.day = in.readInt();
		this.temperature = in.readDouble();
	}

	@Override
	public int compareTo(WeatherWritable o) {
		int result = Integer.compare(this.getYear(),o.getYear());
        if(result == 0)
        {
            result = Integer.compare(this.getMonth(),o.getMonth());
            if(result == 0)
            {
                return -Double.compare(this.getTemperature(), o.getTemperature());
            }
            else
                return result;
        }
        else
            return result;
		
	}

}
