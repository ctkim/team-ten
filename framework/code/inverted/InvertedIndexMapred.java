package inverted;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.umd.cloud9.collection.wikipedia.WikipediaPageInputFormat;
import lemma.LemmaIndexMapred;
import lemma.LemmaIndexMapred.LemmaIndexMapper;
import util.StringIntegerList;
import util.StringIntegerList.StringInteger;

/**
 * This class is used for Section C.2 of assignment 1. You are supposed to run
 * the code taking the lemma index filename as input, and output being the
 * inverted index.
 */
public class InvertedIndexMapred extends Configured implements Tool{
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

		private static final String OUTPUT_PATH = "intermediate_output";

		public int run(String[] args) throws Exception {
			/*job2 - LemmaIndex*/
			Configuration conf = getConf();
			//FileSystem fs = FileSystem.get(conf);
			Job job2 = Job.getInstance(conf, "indexMapred");
			job2.setJarByClass(LemmaIndexMapred.class);

			job2.setInputFormatClass(WikipediaPageInputFormat.class);

			job2.setMapperClass(LemmaIndexMapred.LemmaIndexMapper.class);
			job2.setNumReduceTasks(0);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			FileInputFormat.setInputPaths(job2, new Path(args[0]));
			FileOutputFormat.setOutputPath(job2, new Path(OUTPUT_PATH));

			job2.waitForCompletion(true);


			/*job1 - InvertedIndex */
			Job job = Job.getInstance(conf, "Inverted IndexMapred");
			job.setJarByClass(InvertedIndexMapred.class);

			job.setMapperClass(InvertedIndexMapper.class);
			job.setReducerClass(InvertedIndexReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(StringInteger.class);

			job.setInputFormatClass(KeyValueTextInputFormat.class);

			FileInputFormat.setInputPaths(job, new Path(OUTPUT_PATH));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			return job.waitForCompletion(true) ? 0 : 1;
		}

		public static void main(String[] args) throws Exception {
			// TODO: you should implement the Job Configuration and Job call
			// here
			if (args.length != 2){
				System.err.println("Enter valid number of arguments <Inputdirectory> <Outputlocation>");
				System.exit(0);
			}
			ToolRunner.run(new Configuration(), new InvertedIndexMapred(), args);
		}
}
