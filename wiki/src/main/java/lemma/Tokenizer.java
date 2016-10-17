package lemma;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Tokenizer {

	private static final RegexLibrary regexLibrary = new RegexLibrary();
	private static final String DELIMITER = " \t\n\r\f:;?![]'<>|.,{}=~!@#$^&*_+\\/()“”";

	public Tokenizer() {
		// TODO Auto-generated constructor stub
	}

	//James
	public List<String> tokenize(String sentence) {
		System.out.println(sentence);
		sentence = removeReference(sentence);
		System.out.println(sentence);

		StringTokenizer tokenizer = new StringTokenizer(sentence, DELIMITER);//Deals with bold, italics, and styles.
		List<String> output = new LinkedList<String>();

		while(tokenizer.hasMoreTokens()){
			output.add(tokenizer.nextToken());
		}
		System.out.println(output);
		return output;
	}

	/**
	 * Remove parts of sentence with predefined patterns
	 * @param sentence
	 * @param regexList
	 * @return
	 */
	private String removeWithPattern(String sentence, Map<Pattern, Integer> regexMap){
		Set<Pattern> patterns = regexMap.keySet();
		for(Pattern p: patterns){
			Matcher m = p.matcher(sentence);
			int group = regexMap.get(p);
			if(group != -1){
				sentence = m.replaceAll("$"+group);
			}else{
				sentence = m.replaceAll(" ");
			}
		}
		return sentence;
	}

	/**
	 * removes all references (citations, links, etc) from input
	 * @param sentence
	 * @return
	 */
	private String removeReference(String sentence){
		Map<Pattern, Integer> regexMap = regexLibrary.getReferencePatterns();
		return removeWithPattern(sentence, regexMap);
	}

	//Ben
	private List<String> lemmatize(List<String> tokens) {
		return null;
	}

	//Christine
	private List<String> stopword(List<String> tokens) {
		return null;
	}
}
