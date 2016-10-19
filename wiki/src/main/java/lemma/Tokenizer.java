package lemma;

import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import edu.stanford.nlp.simple.*;

public class Tokenizer {

	private static final RegexLibrary regexLibrary = new RegexLibrary();
	private static final String DELIMITER = " \t\n\r\f:;?![]'<>|.,{}=~!@#$^&*_+\\/()";

	public Tokenizer() {
		// TODO Auto-generated constructor stub
	}

	//James
	public List<String> tokenize(String sentence) throws FileNotFoundException {
		//System.out.println(sentence);
		sentence = removeReference(sentence);
		//System.out.println(sentence);

		StringTokenizer tokenizer = new StringTokenizer(sentence, DELIMITER);//Deals with bold, italics, and styles.
		List<String> output = new LinkedList<String>();

		while(tokenizer.hasMoreTokens()){
			output.add(tokenizer.nextToken());
		}
		//System.out.println(output);
		// stopword removal 
		output = stopword(output);
		//System.out.println(output);
		output = lemmatize(output);
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
		Sentence sent = new Sentence(tokens);
		return sent.lemmas();
	}

	//Christine
	private List<String> stopword(List<String> tokens) throws FileNotFoundException {
		StopWordLibrary lib = new StopWordLibrary();
		HashSet<String> stopWords = lib.getStopWords();
		List<String> noStopWords = new LinkedList<String>();
		for (String s : tokens) {
			if (!stopWords.contains(s)) {
				noStopWords.add(s);
			}
		}
		return noStopWords;
	}
}
