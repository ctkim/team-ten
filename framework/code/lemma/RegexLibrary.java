package tokenizer;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class RegexLibrary {

	private static Map<Pattern, Integer> referencePatterns = new HashMap<>();

	private static final String LINK = "(&lt;ref name=)(.*?)(\\{\\{cite web|url=)(.*?)(\\}\\}&lt;/ref&gt;)";
	private static final String REF = "&lt;ref name=(.*?)/&gt;";
	private static final String QUOTE = "&quot;"; // (.*?) match once or none.
	private static final String GENERAL_REF = "&lt;ref&gt;(.*?)lt;/ref&gt;";
	private static final String FILE = "\\[\\[File:(.*?)\\|(.*?)\\]\\]";
	private static final String INTERNAL_LINK = "\\[\\[(.*?)\\|(.*?)\\]\\]";
	private static final String INTERNAL_LINK2 = "\\{\\{(.*?)\\|(.*?)\\}\\}";
	private static final String BLOCK_QUOTE = "&lt;blockquote&gt;(.*?)&lt;/blockquote&gt;";


	public RegexLibrary(){

		referencePatterns.put(Pattern.compile(LINK), -1);
		referencePatterns.put(Pattern.compile(QUOTE), -1);
		referencePatterns.put(Pattern.compile(GENERAL_REF), -1);
		referencePatterns.put(Pattern.compile(FILE), -1);
		//referencePatterns.put(Pattern.compile(INTERNAL_LINK), 2); //TODO:COMPATIBILITY ISSUE with other regex, needs fix
		referencePatterns.put(Pattern.compile(INTERNAL_LINK2), 2);
		referencePatterns.put(Pattern.compile(BLOCK_QUOTE), 1);
		referencePatterns.put(Pattern.compile(REF), -1);

	}

	public Map<Pattern, Integer> getReferencePatterns(){
		return referencePatterns;
	}

}
