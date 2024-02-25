package Corpus_Folder_Ingestion;

import java.io.File;

public class main {

	public static void main(String args[]) {
		System.out.println("\t##Corpus share-folder automated ingestion module##");
		
		String path = ".";
		String corpusURL = "https://datacorpus.marvel-platform.eu/backend/";
		String user = "";
		String pass = "";
		Boolean appendDataset = false;
		Boolean inferenceResult = false;
		Boolean deleteUploadedSnippet = false;
		Boolean isAugmented = false;
		
		if(args.length==1) { // java -jar CorpusIngestor.jar [path]
			path=args[0];
		}
		for(int i=0;i<args.length;i++) {
			if(args[i].equals("-pt") && ((i+1)<args.length))
				path=args[i+1];
			else if(args[i].equals("-un") && ((i+1)<args.length))
				user=args[i+1];
			else if(args[i].equals("-ps") && ((i+1)<args.length))
				pass=args[i+1];
			else if(args[i].equals("-ip") && ((i+1)<args.length))
				corpusURL=args[i+1];
			else if(args[i].equals("-append"))
				appendDataset=true;
			else if(args[i].equals("-delete"))
				deleteUploadedSnippet=true;
			else if(args[i].equals("-augmented"))
				isAugmented=true;
			else if(args[i].equals("--help") || args[i].equals("-man"))
				System.out.println("\n\t\tMARVEL Corpus command line ingestion client Manual"
						+ "\n-pt\tDefine the path to a folder for the ingestion. The  folder must contains a 'Configuraiton.json' file, defining the details of the dataset that will be ingested to the Corpus."
						+ "\n-ip\tThe IP address (or URL) of the Corpus service. The default value is: https://datacorpus.marvel-platform.eu/backend/"
						+ "\n-un\tThe 'username' of the authenticated user that performs the ingestion. You have to contact the MARVEL team in order to create an account."
						+ "\n-ps\tThe 'password' of the authenticated user that performs the ingestion."
						+ "\n-append\tAppend a dataset entry with new files. If the entry does not exist, create a new one.\n"
						+ "\n-delete\tDelete the snippet files that are uploaded.\n"
						+ "\n--help or -man\tThis manual page.\n");
		}
		
			// Corpus ingestion client 
		CorpusFolderIngestion CFI = new CorpusFolderIngestion(user,pass,corpusURL,appendDataset,deleteUploadedSnippet,isAugmented);
		
		long startTime = System.nanoTime();
			// Automated ingestion of snippets & Benchmarking
		CFI.automatedIngestion(path);
		long elapsedTime = System.nanoTime() - startTime;
		double totalTime = elapsedTime/60000000000.0;
		System.out.println("Total execution time in minutes: " + totalTime);
	}
}
