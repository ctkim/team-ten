package code.lemma;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.StringIntegerList;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

/**
 * 
 *
 */
public class LemmaIndexMapred {
	public static class LemmaIndexMapper extends Mapper<LongWritable, WikipediaPage, Text, StringIntegerList> {

		List<E> <String> a;
		HashMap <String, int> count_token;
		String frequency = "";
		
		@Override
		public void map(LongWritable offset, WikipediaPage page, Context context) throws IOException,
				InterruptedException {
			// TODO: implement Lemma Index mapper here
			a = new ArrayList<String>();
			count_token = new HashMap<String, int>();
			for(int i; i < a.size(); i++){
				if(count_token.contains(a.get(i))){
					count_token.put(a.get(i),count_token.get(a.get(i))+1);
				}else{
					count_token.put(a.get(i),1);
				}
			}
			Iterator<E> it = count_token.entrySet().iterator();
			while(it.hasNext()){
				Map.Entry pair = (Map.Entry)it.next();
				frequency += "<" + pair.getKey() + "," +pair.getValue() + ">";
				it.remove();
			}
			context.write(new Text(page.getContent()), new Text(frequency));
			
		}
	}
}
