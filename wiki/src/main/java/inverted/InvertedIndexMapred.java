package inverted;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import lemma.LemmaIndexMapred;
import lemma.LemmaIndexMapred.LemmaIndexMapper;
import util.StringIntegerList;
import util.StringIntegerList.StringInteger;

/**
 * This class is used for Section C.2 of assignment 1. You are supposed to run
 * the code taking the lemma index filename as input, and output being the
 * inverted index.
 */
public class InvertedIndexMapred {
	public static class InvertedIndexMapper extends Mapper<Text, Text, Text, StringInteger> {

		@Override
		public void map(Text articleId, Text indices, Context context) throws IOException,
				InterruptedException {
			
			String indiceString = indices.toString();
			StringIntegerList s = new StringIntegerList();
			s.readFromString(indiceString);
			List<StringInteger> a = s.getIndices();
			
			for(int i = 0; i < a.size() ; i++){
				
				StringInteger emit_value = new StringInteger(articleId.toString(),a.get(i).getValue());
				context.write(new Text(a.get(i).getString()), emit_value);
			}
			
		}
	}

	public static class InvertedIndexReducer extends
			Reducer<Text, StringInteger, Text, StringIntegerList> {
		@Override
		public void reduce(Text lemma, Iterable<StringInteger> articlesAndFreqs, Context context)
				throws IOException, InterruptedException {
			List<StringInteger> a = new ArrayList<StringInteger>();
			for (StringInteger val : articlesAndFreqs){
				a.add(val);
			}
			StringIntegerList s = new StringIntegerList(a);
			context.write(lemma, s);
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO: you should implement the Job Configuration and Job call
		// here
		Configuration conf = new Configuration();		
		Job job2 = Job.getInstance(conf, "indexMapred");
		job2.setMapperClass(LemmaIndexMapred.LemmaIndexMapper.class);
		FileOutputFormat.setOutputPath(job2, new Path("intermediate_output.txt"));
		
		
		Job job = Job.getInstance(conf, "Inverted IndexMapred");
		job.setJarByClass(InvertedIndexMapred.class);
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringInteger.class);
//		for (int i = 0; i < args.length - 1; ++i){
//			FileInputFormat.addInputPath(job, new Path(args[i]));
//		}
		FileInputFormat.setInputPaths(job, new Path("intermediate_output.txt"));
		FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
