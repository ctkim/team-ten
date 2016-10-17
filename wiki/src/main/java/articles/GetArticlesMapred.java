package articles;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.jar.JarFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import util.WikipediaPageInputFormat;

/**
 * This class is used for Section A of assignment 1. You are supposed to
 * implement a main method that has first argument to be the dump wikipedia
 * input filename , and second argument being an output filename that only
 * contains articles of people as mentioned in the people auxiliary file.
 */
public class GetArticlesMapred {

	//@formatter:off
	/**
	 * Input:
	 * 		Page offset 	WikipediaPage
	 * Output
	 * 		Page offset 	WikipediaPage
	 *
	 */
	//@formatter:on
	public static class GetArticlesMapper extends Mapper<LongWritable, WikipediaPage, Text, Text> {
		public static Set<String> peopleArticlesTitles = new HashSet<String>();
		public static String people = "people.txt";

		@Override
		protected void setup(Mapper<LongWritable, WikipediaPage, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// Populate set with people.txt names
			ClassLoader cl = GetArticlesMapred.class.getClassLoader();
			String fileURL = cl.getResource(people).getFile();
			String jarURL = fileURL.substring(5, fileURL.length() - people.length() - 2);
			JarFile jf = new JarFile(new File(jarURL));
			Scanner sc = new Scanner(jf.getInputStream(jf.getEntry(people)));
			while (sc.hasNextLine()) {
				peopleArticlesTitles.add(sc.nextLine());
			}
			jf.close();
			sc.close();
			super.setup(context);
		}

		@Override
		public void map(LongWritable offset, WikipediaPage inputPage, Context context)
				throws IOException, InterruptedException {
			// check if title is a person name
			if (peopleArticlesTitles.contains(inputPage.getTitle())) {
				context.write(new Text(""), new Text(inputPage.getContent()));
			}
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
        	System.err.println("Usage: getarticlesmapred <in> <out>");
        	System.exit(2);
        }
        
		Job job = Job.getInstance(conf, "get articles");
		// we have a custom input format
		job.setInputFormatClass(WikipediaPageInputFormat.class);
        job.setJarByClass(GetArticlesMapred.class);
        job.setMapperClass(GetArticlesMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
