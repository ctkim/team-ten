package lemma;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringInterner;

import util.StringIntegerList;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

/**
 * 
 *
 */
public class LemmaIndexMapred {
	public static class LemmaIndexMapper extends Mapper<LongWritable, WikipediaPage, Text, StringIntegerList> {

		StringIntegerList s;
		Tokenizer tokenizer;
		List<String> a;
		Map<String, Integer> map;
		
		@Override
		public void map(LongWritable offset, WikipediaPage page, Context context) throws IOException,
				InterruptedException {
			
			tokenizer = new Tokenizer();
			a = tokenizer.tokenize(page.getContent());
			
			map = new HashMap<String, Integer>(); 
			 
			for(int i = 0; i < a.size(); i++){
				if(map.containsKey(a.get(i))){
					map.put(a.get(i),map.get(a.get(i))+1);
				}else{
					map.put(a.get(i),1);
				}
			}
			
			s = new StringIntegerList(map);
			
			context.write(new Text(page.getTitle()), s);
			
		}
	}
}
