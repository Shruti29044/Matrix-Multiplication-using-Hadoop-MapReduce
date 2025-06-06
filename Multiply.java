import java.io.*;
import java.util.*;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.ReflectionUtils;

class Elem implements Writable {
//here tag indicates 0 for Matrix M and 1 for Matrix N, index for key pairs

	int tag;
	int index;
	double value;
	
	Elem() {}
	
	Elem(int tag, int index, double value) {
		this.tag = tag;
		this.index = index;
		this.value = value;
	}
	
	@Override
	public void readFields(DataInput input) throws IOException {
	//for reading the input
		tag = input.readInt();
		index = input.readInt();
		value = input.readDouble();
	}
	
	@Override
	public void write(DataOutput output) throws IOException {
	//for writing out the output
		output.writeInt(tag);
		output.writeInt(index);
		output.writeDouble(value);
	}
}

class Pair implements WritableComparable<Pair> {

	int i;
	int j;
	
	Pair() {}
	
	Pair(int i, int j) {
		this.i = i;
		this.j = j;
	}
	
	@Override
	public void readFields(DataInput input) throws IOException {
		i = input.readInt();
		j = input.readInt();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeInt(i);
		output.writeInt(j);
	}	
	
	@Override
	public int compareTo(Pair pr) {
		
		if (i > pr.i) {
			return 1;
		} else if ( i < pr.i) {
			return -1;
		} else {
			if(j > pr.j) {
				return 1;
			} else if (j < pr.j) {
				return -1;
			}
		}
		return 0;
	}
	
	@Override
	public String toString() {
		return i + " " + j + " ";
	}
	
}

public class Multiply {

//Mapper for Matrix M
	
	public static class MMatriceMapper extends Mapper<Object,Text,IntWritable,Elem> {
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String readLine = value.toString();
			String[] stringTokens = readLine.split(",");
			
			int i = Integer.parseInt(stringTokens[0]);
			double v = Double.parseDouble(stringTokens[2]);

			Elem e = new Elem(0, i, v);

			IntWritable keyValue = new IntWritable(Integer.parseInt(stringTokens[1]));
			context.write(keyValue, e);
		}
	}

//Mapper for Matrix N
	public static class NMatriceMapper extends Mapper<Object,Text,IntWritable,Elem> {
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String readLine = value.toString();
			String[] stringTokens = readLine.split(",");
			
			int j = Integer.parseInt(stringTokens[1]);
			double v = Double.parseDouble(stringTokens[2]);

			Elem e = new Elem(1,j, v);
			
			IntWritable keyValue = new IntWritable(Integer.parseInt(stringTokens[0]));
			context.write(keyValue, e);
		}
	}

//First Reducer for Matrix MUltiplication where Intermediate values are generated

	public static class MNMatrixReducer extends Reducer<IntWritable,Elem, Pair, DoubleWritable> {
	
	@Override
	public void reduce(IntWritable key, Iterable<Elem> values, Context context) throws IOException, InterruptedException {
		
		ArrayList<Elem> M = new ArrayList<Elem>();
		ArrayList<Elem> N = new ArrayList<Elem>();
		
		Configuration conf = context.getConfiguration();
		
		for(Elem Elem : values) {
			
			Elem tempElem = ReflectionUtils.newInstance(Elem.class, conf);
			ReflectionUtils.copy(conf, Elem, tempElem);
			
			if (tempElem.tag == 0) {
				M.add(tempElem);
			} else if(tempElem.tag == 1) {
				N.add(tempElem);
			}
		}
		
		for(int i=0;i<M.size();i++) {
			for(int j=0;j<N.size();j++) {
				
				Pair p = new Pair(M.get(i).index,N.get(j).index);
				double matrixmultiplication = M.get(i).value * N.get(j).value;

				context.write(p, new DoubleWritable(matrixmultiplication));
				}
			}
		}
	}
	
//SEcond Mapper for Matrix Multiplication

	public static class FinalMNMatrixMapper extends Mapper<Object, Text, Pair, DoubleWritable> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String readLine = value.toString();
			String[] pairValue = readLine.split(" ");
			
			Pair p = new Pair(Integer.parseInt(pairValue[0]),Integer.parseInt(pairValue[1]));
			DoubleWritable val = new DoubleWritable(Double.parseDouble(pairValue[2]));

			context.write(p, val);
		}
	}
	
//Second REducer for Matrix Multiplication

	public static class FinalMNMatrixReducer extends Reducer<Pair, DoubleWritable, Pair, DoubleWritable> {
		@Override
		public void reduce(Pair key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			
			double finalmatrixsum = 0.0;
			for(DoubleWritable value : values) {
				finalmatrixsum += value.get();
			}

			context.write(key, new DoubleWritable(finalmatrixsum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		
//First Map Reduce Job
		
		Job job = Job.getInstance();
		job.setJobName("MapIntermediate");
		job.setJarByClass(Multiply.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MMatriceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, NMatriceMapper.class);
		job.setReducerClass(MNMatrixReducer.class);

		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Elem.class);
		
		job.setOutputKeyClass(Pair.class);
		job.setOutputValueClass(DoubleWritable.class);
		
//generating intermediate values after first map reduce job
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
		
		
//Second Map reduce job		
		Job job2 = Job.getInstance();
		job2.setJobName("MapFinalOutput");
		job2.setJarByClass(Multiply.class);
		
		job2.setMapperClass(FinalMNMatrixMapper.class);
		job2.setReducerClass(FinalMNMatrixReducer.class);
		
		job2.setMapOutputKeyClass(Pair.class);
		job2.setMapOutputValueClass(DoubleWritable.class);
		
		job2.setOutputKeyClass(Pair.class);
		job2.setOutputValueClass(DoubleWritable.class);

//Reading intermediate values from first map reduce
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job2, new Path(args[2]));
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));

		
		job2.waitForCompletion(true);
	}
}
