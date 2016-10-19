package lemma;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.Scanner;

public class StopWordLibrary {
	
	private static HashSet<String> stopWords = new HashSet<String>();
	private static final String wordList = "src/main/java/lemma/stopwordlist.txt";
	
	public StopWordLibrary() throws FileNotFoundException {
		Scanner sc = new Scanner(new File(wordList));
		while (sc.hasNextLine()) {
			stopWords.add(sc.nextLine().trim());
		}
		sc.close();
	}
	
	public HashSet<String> getStopWords() {
		return stopWords;
	}

}
