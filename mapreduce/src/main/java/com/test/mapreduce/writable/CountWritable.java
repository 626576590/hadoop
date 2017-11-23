package com.test.mapreduce.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CountWritable implements WritableComparable<CountWritable> {
	
	public Text word;
	public Integer frequency;
	
	public CountWritable(){
		this(new Text(), 1);  
	}
	
	public CountWritable(CountWritable wcp){  
        this(wcp.word,wcp.frequency);  
    }     
  
	
	public CountWritable(Text word, Integer frequency){
		this.word=word;
		this.frequency=frequency;
	}

	public void write(DataOutput out) throws IOException {
		word.write(out);  
        out.writeInt(frequency);
	}

	public void readFields(DataInput in) throws IOException {
		word.readFields(in);
		frequency = in.readInt();
	}

	public int compareTo(CountWritable o) {
		return 0;
	}

}
