package Corpus_Folder_Ingestion;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.FormBodyPart;
import org.apache.http.entity.mime.FormBodyPartBuilder;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.json.*;

import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Request.Builder;
import okhttp3.RequestBody;
import okhttp3.Response;

public class CorpusVideoAnonyIngestion {

	private ArrayList<String> datasetIds;	// the list of dataset IDs in the Corpus. It is used in order to check wherever a dataset already exists in the Corpus and has to be updated. Otherwise a new dataset will be created for this ingestion.
	private ArrayList<String> datasetKeys;	// the list of dataset Keys, similarly with 'datasetIds'
	private String folderName;		// path for snippet ingestion. Starts from './'
	private JSONObject DatasetJSON;	// the JSON file that is created for this ingestion
	private int ingestedSnippets;	// the number of snippets that are created for this ingestion
	private String datasetID;		// the dataset ID for this ingestion
	private String pathDash="\\";	// (internal operation) the dash type discriminates Windows and Linux operating systems
	private String tokenizationType="";	// the file name format that will be used in order to parse the underlying snippet files
	private String username;		// the username for authentication (MARVEL HTTPS)
	private String pass;			// the password of this account (MARVEL HTTPS)
	private Boolean appendDataset;	// checks if the dataset is complete or is to be appended later
	private Boolean deleteUploadedSnippet=false;	// checks if the uploaded snippet files have to be deleted afterwards from the local disk
	private Boolean processingFolder=false;	// checks if the current element that it is processed, is a folder or not. In case where the 'delete' option has been selected, the process has to delete both the folder and the produced zip file.
	Boolean isAugmented = false;	// checks if the dataset is augmented. In that case, if a folder is processed, it will be uploaded as the main snippet file, and not as an annotation file.
	private Long tmpTime = Long.valueOf(1669985902);
	private ArrayList<String> notIngestedFiles;
	private String CorpusURL = "https://datacorpus.marvel-platform.eu/backend/";

		//CONSTRUCTORS
	public CorpusFolderIngestion(String username, String pass, String CorpusURL, Boolean appendDataset, Boolean deleteUploadedSnippet, Boolean isAugmented) {	// start ingestion from the current folder './'
		this.folderName=null;
		this.datasetIds = new ArrayList<String>();
		this.datasetKeys = new ArrayList<String>();
		this.DatasetJSON=null;
		this.ingestedSnippets=0;
		this.datasetID=null;
		this.username=username;
		this.pass=pass;
		this.CorpusURL=CorpusURL;
		this.appendDataset=appendDataset;
		this.deleteUploadedSnippet=deleteUploadedSnippet;
		this.isAugmented=isAugmented;
		this.notIngestedFiles = new ArrayList<String>();
	}
	public CorpusFolderIngestion(String username, String pass, String folderName, Boolean appendDataset, Boolean isAugmented) {	// take as input a folder with the dataset configuration JSON and the snippet files for this ingestion 
		this.folderName=folderName;
		this.datasetIds = new ArrayList<String>();
		this.datasetKeys = new ArrayList<String>();
		this.DatasetJSON=null;
		this.ingestedSnippets=0;
		this.datasetID=null;
		this.username=username;
		this.pass=pass;
		this.appendDataset=appendDataset;
		this.isAugmented=isAugmented;
		this.notIngestedFiles = new ArrayList<String>();
	}
	
		//MAIN (PUBLIC) METHODS
		
		// Automated ingestion for files with a pre-defined naming format (i.e., Source/Device-ID.start_timestamp.end_timestamp.*)
		// Then, it is checked if a related dataset already exists (based on 'Source/Device-ID' and the date from the 'start_timestamp')
		// If a dataset does not exist, a new one is created (with default values for some fileds)
		// Finally, the files are ingested in the Corpus
	
		// Automated ingestion for files that do not follow the naming format. A 'Configuration.json' file is needed to provide the appropriate information to correlate the data with a dataset entry in the Corpus
	public void automatedIngestion(String folderName) {
		Boolean existedDataset=false;
			// if the dataset JSON already exists, parse it and continue ingestion
		if(appendDataset==true) {
			System.out.println("Check for existing dataset entry JSON");
			try {
				File tmpfile1 = new File(folderName+pathDash+"DatasetEntry.json");
				if(tmpfile1.exists()) {
					DatasetJSON = readDatasetTemplateFromFile(tmpfile1.getAbsolutePath());
					existedDataset=true;
				}
				else {
					pathDash="/";
					File tmpfile2 = new File(folderName+pathDash+"DatasetEntry.json");
					if(tmpfile2.exists()) {
						DatasetJSON = readDatasetTemplateFromFile(tmpfile2.getAbsolutePath());
						existedDataset=true;
					}
					else
						pathDash="\\";
				}
			}
			catch(Exception e) {
				System.out.println("\tError while parsing JSON template from file: "+e);
				return;
			}
			
		}
		if(existedDataset==true) {
			//Update snippets count for existing dataset
			ingestedSnippets=countSnippets(DatasetJSON);
		}
		else if(existedDataset==false) {
			// Process the Configuration JSON file. At first try a path for Windows (pathDash="\\"), otherwise for Linux OS (pathDash="/").
			DatasetJSON = readDatasetTemplateFromFile(folderName+pathDash+"Configuration.json");
			if(DatasetJSON==null) {
					// Linux path name
				pathDash="/";
				DatasetJSON = readDatasetTemplateFromFile(folderName+pathDash+"Configuration.json");
				if(DatasetJSON==null) {
					System.out.println("\tError with reading Configuration file");
					return;
				}
			}
		}
		
		startAutomatedIngestion(folderName, DatasetJSON);	// Ingest the snippet files from the given folder
		
		// DEBUG //
		System.out.println("\n=====\nFinal Dataset JSON: "+DatasetJSON.toString()+"\n=====");	// the final JSON file for this ingestion
		System.out.println("\n=====\nFiles not ingested: "+notIngestedFiles.toString()+"\n=====");	// the files that were not ingested due to error
		// DEBUG //
		if(appendDataset==true) {	// Save Dataset Entry file in 'DatasetEntry.json'
			try {
			      FileWriter myWriter = new FileWriter(folderName+pathDash+"DatasetEntry.json");
			      myWriter.write(DatasetJSON.toString());
			      myWriter.close();
			      System.out.println("Save Dataset Entry file\n");
			}
			catch (Exception e) {
			      System.out.println("Error while saving Dataset Entry file: "+e);
			}
		}
	}
	
		//OTHER (PRIVATE) METHODS
	
	//
	// Count how many snippets have been already ingested in an existing dataset that has to be appended now
	//
	
	
	private int countSnippets(JSONObject jbDataset) {
		try {
			JSONObject jbDatasetInfo = jbDataset.getJSONObject("datasetInfo");
			String datasetType = jbDatasetInfo.getString("category");
			JSONObject jbMetaData = jbDataset.getJSONObject(datasetType.toLowerCase());
			JSONArray snippetArray = jbMetaData.getJSONArray("snippets");
			return(snippetArray.length());
		}
		catch(Exception e) {
			System.out.println("Error in counting snippet files for existing dataset entry");
		}
		return(0);
	}
	
	
	//
	// Random Input - Files are not following the File naming format (i.e. GRN_1.startTimestamp.endTimestamp.fileType)
	//
	private int startAutomatedIngestion(String folderName, JSONObject jbDataset) {
			// check if the provided folder name is null
		if(folderName==null) {
			System.err.println("Folder name is NULL");
			return(-1);
		}
			// check if the provided folder name is empty
		if(folderName.isBlank() || folderName.isEmpty()) {
			System.out.println("Checking the current folder for snippet files");
			folderName=".";
		}
			// Read the current dataset IDs list from the Corpus and store them in local variable (datasetIds)
		readDatasetIDsFromCorpus();
		
		String dataProvider, datasetType, datasetJSONID;	// To be used later (e.g., dataset ID creation)
		try {
			JSONObject jbDatasetInfo = jbDataset.getJSONObject("datasetInfo");
			dataProvider = jbDatasetInfo.getString("dataProvider");
			datasetType = jbDatasetInfo.getString("category");
				// Check if the user has specified a dataset ID
			datasetJSONID=jbDatasetInfo.getString("dataset");
			if(!datasetJSONID.isBlank()) {
				datasetID=datasetJSONID;
			}
		}
		catch(Exception e) {
			System.out.println("\tError while parsing Configuration file"+e);
			return(-2);
		}
		
		try {
			File folder = new File(folderName);
			for (final File fileEntry2 : folder.listFiles()) {
				
				File fileEntry = fileEntry2;
				processingFolder=false;
					// Folder with annotation results -> Zip folder and upload the result in Corpus
				if(fileEntry2.isDirectory()) {
					processingFolder=true;
					System.out.println("-New Folder= "+fileEntry2.getName());
						// Zip folder
					ZipUtils appZip = new ZipUtils(fileEntry2.getPath()+".zip", fileEntry2.getPath());
			        appZip.generateFileList(new File(fileEntry2.getPath()));
			        appZip.zipIt(fileEntry2.getPath()+".zip");
			        	// Upload zip file as the annotation file for this snippet
			        fileEntry = new File(fileEntry2.getPath()+".zip");
				}
				
				if (fileEntry.isFile() && !fileEntry.isDirectory() && !fileEntry.getName().endsWith(".json") && !fileEntry.getName().equals("output.txt") && !fileEntry.getName().endsWith(".jar") && !fileEntry.getName().endsWith("~")) {
					if(appendDataset==true && jbDataset.toString().contains(fileEntry.getName())) { // if the dataset is to be appended and this file had been already ingested, then ignore this file
						System.out.println("File ("+fileEntry.getName()+") had already been ingested");
					}
					else {
						System.out.println("\n-----\n-New File= " +fileEntry.getName());
						if(tokenizationType.isBlank()) {
							checkFileNameFormat(fileEntry.getName());
						}
						
							String sourceID, fileType, startTimestamp, endTimestamp, codec, fps="";
							try {
								if(tokenizationType.equals("StreamHandler")){
									String[] datasetElements = fileEntry.getName().split("\\.");
									fileType = datasetElements[1];	// File type
									String[] datasetElements2 = datasetElements[0].split("-");	// Tokenize the file name in 4 parts (SourceID.startTimestamp.endTimestamp.*)
									sourceID = datasetElements2[2];	// sourceID -1/2
									String[] datasetElements3 = datasetElements2[3].split("_");
									sourceID+= "-"+datasetElements3[0];	// sourceID -2/2
									
									startTimestamp = datasetElements3[1];	// startTimestamp
									if(datasetElements3.length==3)
										endTimestamp = datasetElements3[2];	// endTimestamp
									else
										endTimestamp = startTimestamp;
									codec = "";
									
										//fps
									if(datasetType.equalsIgnoreCase("video") || datasetType.equalsIgnoreCase("audio-video")) {
										try {
											JSONObject jbDataType = jbDataset.getJSONObject("video");
											fps = jbDataType.getString("videoFps");
										}
										catch(Exception e) {
											System.out.println("\tError while parsing Configuration file to read fps. Assign default value.");
										}
									}
								}
								else if(tokenizationType.equals("OLDStreamHandler")){
									//FileNameTokenization(fileEntry.getName());
									String[] datasetElementsTMP = fileEntry.getName().split("\\.");	// exclude the last part of the filename (".*")
									String[] datasetElements = datasetElementsTMP[0].split(".");	// Tokenize the file name in 3 parts (SourceID.startTimestamp.endTimestamp)
									sourceID = datasetElements[1];
									startTimestamp = datasetElements[2];
									if(datasetElements.length>4)
										endTimestamp = datasetElements[3];
									else
										endTimestamp = startTimestamp;
									codec = "";
									
										//fps
									if(datasetType.equalsIgnoreCase("video") || datasetType.equalsIgnoreCase("audio-video")) {
										try {
											JSONObject jbDataType = jbDataset.getJSONObject("video");
											fps = jbDataType.getString("videoFps");
										}
										catch(Exception e) {
											System.out.println("\tError while parsing Configuration file to read fps. Assign default value.");
										}
									}
									fileType=datasetElementsTMP[1];
								}
								else if(tokenizationType.equals("VideoAnony")){
									//// VideoAnony ingestion ////
									String[] datasetElements = fileEntry.getName().split("_");	// Tokenize the file name in 4 parts (SourceID.startTimestamp.endTimestamp.*)
									sourceID = datasetElements[1];
									startTimestamp = datasetElements[2];
									endTimestamp = datasetElements[3];
									codec = datasetElements[4];
									String[] technicalDetails = datasetElements[5].split("fps.");
									fps=technicalDetails[0];
									fileType=technicalDetails[1];
								}
								else if(tokenizationType.equals("VideoAnony2")){
									//// VideoAnony2 ingestion ////
									String[] datasetElements = fileEntry.getName().split("_");	// Tokenize the file name in 4 parts (SourceID.startTimestamp.endTimestamp.*)
									sourceID = datasetElements[0];
									startTimestamp = datasetElements[1];
									startTimestamp = startTimestamp.replace("-", "T");
									startTimestamp = startTimestamp.replace(".", "");
									endTimestamp = startTimestamp;
									codec = datasetElements[2];
									String[] technicalDetails = datasetElements[3].split("fps.");
									fps=technicalDetails[0];
									fileType=technicalDetails[1];
								}
								else if(tokenizationType.equals("MT")){
									//// MT ingestion ////
									String[] datasetElements = fileEntry.getName().split("-");	// Tokenize the file name in 4 parts (SourceID.startTimestamp.endTimestamp.*)
									sourceID = datasetElements[1]+"-"+datasetElements[2];
									startTimestamp = datasetElements[3];
									endTimestamp = datasetElements[4];
										//fps
									if(datasetType.equalsIgnoreCase("video") || datasetType.equalsIgnoreCase("audio-video")) {
										try {
											JSONObject jbDataType = jbDataset.getJSONObject("video");
											fps = jbDataType.getString("videoFps");
										}
										catch(Exception e) {
											System.out.println("\tError while parsing Configuration file to read fps. Assign default value.");
										}
									}
									String[] technicalDetails = datasetElements[5].split("\\.");
									codec=technicalDetails[0];
									fileType=technicalDetails[1];
								}
								else if(tokenizationType.equals("UNS")){
									//// UNS ingestion ////
									String[] datasetElements = fileEntry.getName().split("\\.");	// Tokenize the file name in 4 parts (SourceID.startTimestamp.endTimestamp.*)
									sourceID = "drone1";
									startTimestamp=""+tmpTime++;
									endTimestamp=""+tmpTime++;
										//fps
									if(datasetType.equalsIgnoreCase("video") || datasetType.equalsIgnoreCase("audio-video")) {
										try {
											JSONObject jbDataType = jbDataset.getJSONObject("video");
											fps = jbDataType.getString("videoFps");
										}
										catch(Exception e) {
											System.out.println("\tError while parsing Configuration file to read fps. Assign default value.");
										}
									}
									codec="mp4";
									fileType=datasetElements[1];
								}
								else if(tokenizationType.equals("GRN")){
									//// GRN ingestion ////
									String[] datasetElements = fileEntry.getName().split("\\.");
									fileType = datasetElements[1];	// File type
									codec=fileType;
									String[] datasetElements2 = datasetElements[0].split("_");
									String time = datasetElements2[1];
									//String anonymizationStatus = datasetElements2[2];
									String tmp = datasetElements2[0];
									sourceID = datasetElements2[0].substring(0, datasetElements2[0].length()-10);
									String strDate = datasetElements2[0].substring(datasetElements2[0].length()-10, datasetElements2[0].length()) + " " +time;
										
									Date tmpDate = new SimpleDateFormat("yyyy-MM-dd HH-mm-ssZ").parse(strDate);
									startTimestamp = Long.toString(tmpDate.getTime());
									endTimestamp=startTimestamp;
										//fps
									if(datasetType.equalsIgnoreCase("video") || datasetType.equalsIgnoreCase("audio-video")) {
										try {
											JSONObject jbDataType = jbDataset.getJSONObject("video");
											fps = jbDataType.getString("videoFps");
										}
										catch(Exception e) {
											System.out.println("\tError while parsing Configuration file to read fps. Assign default value.");
										}
									}
								}
								else if(tokenizationType.equals("Simple")){
									//// Simple ingestion ////
									String[] datasetElements = fileEntry.getName().split("\\.");	// exclude the last part of the filename (".*")
									sourceID = datasetElements[0];
									fileType=datasetElements[1];
									
									if(checkSnippetName(folder,sourceID)!=0) {
										startTimestamp=""+(tmpTime-2);
										endTimestamp=""+(tmpTime-1);
									}
									else {
										startTimestamp=""+tmpTime++;
										endTimestamp=""+tmpTime++;
									}
									codec="";
									fps="";
								}
								else {
									System.err.println("\tError with file name tokenization\nFile Name: "+fileEntry.getName());
									break;
								}
							}
							catch(Exception e) {
								System.err.println("\tError with file name tokenization\nFile Name: "+fileEntry.getName()+"\nError: "+e);
								break;
							}
							
							int ingestionResult;
							ingestionResult=ingestFile(folderName,fileEntry,dataProvider,sourceID,datasetType,fileType,startTimestamp,endTimestamp,fps);
							if(ingestionResult==0) {	// If file has been ingested, then delete local file
								if(deleteUploadedSnippet==true) {
									if(processingFolder==true) {
										String tmp = ""+fileEntry.getAbsoluteFile();
										String zipFolderName=tmp.substring(0, fileEntry.getAbsoluteFile().toString().length()-4);

										File zipFolder = new File(zipFolderName);
										deleteDir(zipFolder);
										System.out.println("Delete folder: "+zipFolderName);
									}
									System.out.println("Delete file: "+fileEntry.getAbsoluteFile());
									fileEntry.delete();
								}
							}
							else {	// If file has not been ingested, then delete snippet entry from dataset's JSON
								deleteSnippet(fileEntry.getName(), datasetType);
								notIngestedFiles.add(fileEntry.getName());
							}
						}
				}
			}
		}
		catch(Exception e) {
			System.err.println("\tError while reading the contents of the folder ("+folderName+")\n"+e);
			return(-3);
		}
		return(0);	// normal exit sequence
	}
	
		// Assistive Method to delete a folder along with the underlying files 
	private void deleteDir(File file) {
	    File[] contents = file.listFiles();
	    if (contents != null) {
	        for (File f : contents) {
	            deleteDir(f);
	        }
	    }
	    file.delete();
	}
	
		// Assistive Method for 'Simple' ingestion type - Checks if a snippet has more than one files (e.g., snippet file, annotations, and inference results)
	private int checkSnippetName(File folder, String snippetName) {
		int count=0;
		try {
			for (final File fileEntry : folder.listFiles())
				if(fileEntry.getName().equals(snippetName))
					count++;
		}
		catch(Exception e) {
			System.err.println("Error in parsing file list: "+e);
		}
		return(count);
	}
	
	private String checkFileNameFormat(String fileEntryName) {
		if(isFileNameFormatCompliantStreamHandler(fileEntryName))
			tokenizationType="StreamHandler";	// "Cam-GRN-VA-02_1671623325_1671623335.*"
		else if(isFileNameFormatCompliantGRN(fileEntryName))
			tokenizationType="GRN";		// "greenroads-streams1-stream12022-09-06_11-55-24+0200_anonymized.wav"
		else if(isFileNameFormatCompliantVideoAnony(fileEntryName))
			tokenizationType="VideoAnony";		// "rec_piazza-fiera-3_20220430T121805_20220430T122105_mp4v_12.5fps.avi"
		else if(isFileNameFormatCompliantVideoAnony2(fileEntryName))
			tokenizationType="VideoAnony2";		// "gozo-channel-104_2023.04.21-05.21.21_mpeg4_10fps.mp4"
		else if(isFileNameFormatCompliantMT(fileEntryName))
			tokenizationType="MT";				// "rec-dogana-2-20211207T074228-20211207T074528-mjpeg.mp4"
		else if(isFileNameFormatCompliantUNS(fileEntryName))
			tokenizationType="UNS";				// "Video190_segment_1.rar" or "mix_0.zip"
		else if(OLDisFileNameFormatCompliantStreamHandler(fileEntryName))
			tokenizationType="OLDStreamHandler";	// "DeviceID.StartTimestamp.EndTimestamp.*"
		else
			tokenizationType="Simple";			// "1.mp4"
		//System.out.println("TokenizationType= "+tokenizationType);
		return(tokenizationType);
	}
	
	//Check if the file name complies with the defined format (e.g., "gozo-channel-104_2023.04.21-05.21.21_mpeg4_10fps.mp4")
	private boolean isFileNameFormatCompliantUNS(String fileEntryName) {
		if(fileEntryName.startsWith("Video") || fileEntryName.startsWith("mix_"))
			if(fileEntryName.endsWith(".rar") || fileEntryName.endsWith(".zip"))
				return(true);
		return(false);
	}
	
	//Check if the file name complies with the defined format (e.g., "gozo-channel-104_2023.04.21-05.21.21_mpeg4_10fps.mp4")
	private boolean isFileNameFormatCompliantVideoAnony2(String fileEntryName) {
		String[] results = new String[6];
		try {
			String[] datasetElements = fileEntryName.split("_");	// Tokenize the file name in 4 parts (SourceID.startTimestamp.endTimestamp.*)
			results[0] = datasetElements[0];	// sourceID
			results[1] = datasetElements[1];	// startTimestamp
			results[1] = results[1].replace("-", "T");
			results[1] = results[1].replace(".", "");
			results[2] = results[1];	// endTimestamp
			results[3] = datasetElements[2];	// codec
			String[] technicalDetails = datasetElements[3].split("fps.");
			results[4]=technicalDetails[0];	// fps
			results[5]=technicalDetails[1];	// fileType
		}
		catch (Exception e){
			return(false);
		}
		return(true);	// file name does not comply with the defined format
	}
	
		//Check if the file name complies with the defined format ("DeviceID.StartTimestamp.EndTimestamp.*" or "DeviceID.StartTimestamp.*")
	private boolean OLDisFileNameFormatCompliantStreamHandler(String fileEntryName) {
		//(OLD)//String pattern = "[a-zA-Z0-9]+[_]+[0-9]+[_]+[0-9]+.*"; //regular expression to check the file name format -> i.e., [String]-[number].[number].*
		String pattern = "[a-zA-Z0-9_]+.[0-9]+.[0-9]+.*"; //regular expression to check the file name format -> i.e., [String]-[number].[number].*
		if(fileEntryName.matches(pattern))
			return(true);	// file name complies with the defined format
		pattern = "[a-zA-Z0-9_]+.[0-9]+.*"; //regular expression to check the file name format -> i.e., [String]-[number].[number].*
		if(fileEntryName.matches(pattern))	// No endTimestamp
			return(true);	// file name complies with the defined format
		return(false);	// file name does not comply with the defined format
	}
	
	//Check if the file name complies with the defined format ("DeviceID.StartTimestamp.EndTimestamp.*" or "DeviceID.StartTimestamp.*")
	private boolean isFileNameFormatCompliantStreamHandler(String fileEntryName) {
		String[] results = new String[6];
		try {
			String[] datasetElements = fileEntryName.split("\\.");
			results[5] = datasetElements[1];	// File type
			String[] datasetElements2 = datasetElements[0].split("-");	// Tokenize the file name in 4 parts (SourceID.startTimestamp.endTimestamp.*)
			results[0] = datasetElements2[0];	// Device type, e.g., CAM
			results[1] = datasetElements2[1];	// Pilot
			results[2] = datasetElements2[2];	// sourceID -1/2
			String[] datasetElements3 = datasetElements2[3].split("_");
			results[2]+= "-"+datasetElements3[0];	// sourceID -2/2
			results[3] = datasetElements3[1];	// startTimestamp
			
			if(datasetElements3.length==3)
				results[4] = datasetElements3[2];	// endTimestamp
			else
				results[4] = results[3];
		}
		catch (Exception e){
			return(false);
		}
		return(true);	// file name does not comply with the defined format
	}
	
	//Check if the file name complies with the defined format ("[stream_name][date]_[time stamp]_anonymized.wav.*")
	private boolean isFileNameFormatCompliantGRN(String fileEntryName) {
		// "greenroads-streams1-stream12022-09-06_11-55-24+0200_anonymized.wav"
		String[] results = new String[5];
		results[0] = "CAM";
		results[1] = "GRN";
		try {
			String[] datasetElements = fileEntryName.split("\\.");
			results[4] = datasetElements[1];	// File type
			
			String[] datasetElements2 = datasetElements[0].split("_");
			String time = datasetElements2[1];
			String anonymizationStatus = datasetElements2[2];
			String tmp = datasetElements2[0];
			results[2] = datasetElements2[0].substring(0, datasetElements2[0].length()-10);
			results[3] = datasetElements2[0].substring(datasetElements2[0].length()-10, datasetElements2[0].length()) + " " +time;
			
			Date tmpDate = new SimpleDateFormat("yyyy-MM-dd HH-mm-ssZ").parse(results[3]);
			//System.out.println("tmpDate: "+tmpDate);
			results[3] = Long.toString(tmpDate.getTime());
		}
		catch (Exception e){
			System.err.println("Error in 'isFileNameFormatCompliantGRN': "+e);
			return(false);
		}
		return(true);	// file name does not comply with the defined format
	}
		
	
		//Check if the file name complies with the defined format (e.g., "rec_piazza-fiera-3_20220430T121805_20220430T122105_mp4v_12.5fps.avi")
	private boolean isFileNameFormatCompliantVideoAnony(String fileEntryName) {
		String[] results = new String[6];
		try {
			String[] datasetElements = fileEntryName.split("_");	// Tokenize the file name in 4 parts (SourceID.startTimestamp.endTimestamp.*)
			results[0] = datasetElements[1];	// sourceID
			results[1] = datasetElements[2];	// startTimestamp
			
			if(datasetElements.length==6) {
				results[2] = datasetElements[3];	// endTimestamp
				results[3] = datasetElements[4];	// codec
				String[] technicalDetails = datasetElements[5].split("fps.");
				results[4]=technicalDetails[0];	// fps
				results[5]=technicalDetails[1];	// fileType
			}
			else {
				results[2] = datasetElements[2];	// endTimestamp
				results[3] = datasetElements[3];	// codec
				String[] technicalDetails = datasetElements[4].split("fps.");
				results[4]=technicalDetails[0];	// fps
				results[5]=technicalDetails[1];	// fileType
			}
		}
		catch (Exception e){
			return(false);
		}
		return(true);	// file name does not comply with the defined format
	}
	
		//Check if the file name complies with the defined format (e.g., "rec_piazza-fiera-3_20220430T121805_20220430T122105_mp4v_12.5fps.avi")
	private boolean isFileNameFormatCompliantMT(String fileEntryName) {
		String[] results = new String[6];
		try {
			String[] datasetElements = fileEntryName.split("-");	// Tokenize the file name in 4 parts (SourceID.startTimestamp.endTimestamp.*)
			results[0] = datasetElements[1]+"-"+datasetElements[2];	// sourceID
			results[1] = datasetElements[3];	// startTimestamp
			results[2] = datasetElements[4];	// endTimestamp
			results[3]="12";	// fps
			String[] technicalDetails = datasetElements[5].split("\\.");
			results[4]=technicalDetails[0];	// codec
			results[5]=technicalDetails[1];	// fileType
		}
		catch (Exception e){
			return(false);
		}
		return(true);	// file name does not comply with the defined format
	}
	
	
	//
	// StreamHandler Input - Files are following the File naming format (i.e. GRN_1.startTimestamp.endTimestamp.fileType)
	//
	private String[] FileNameTokenization(String fileEntryName) {
		String [] datasetElements=null;
		String sourceID, fileType, startTimestamp, endTimestamp;
		try {
				//String[] datasetElementsTMP = fileEntryName.split("\\.");	// exclude the last part of the filename (".*")
				//String[] datasetElements = datasetElementsTMP[0].split(".");	// Tokenize the file name in 3 parts (SourceID.startTimestamp.endTimestamp)
			datasetElements = fileEntryName.split("_");	// Tokenize the file name in 4 parts (SourceID.startTimestamp.endTimestamp.*)
			System.out.println("Tokens: "+datasetElements.length);
			sourceID = datasetElements[0];
			startTimestamp = "1";
			endTimestamp = "2";
			fileType = datasetElements[3];
		}
		catch(Exception e) {
			System.err.println("\tError with file name tokenization\n"+e);
		}
		return(datasetElements);
	}
	

	
	//
	//GENERAL METHODS
	//
		// Upload file in the Corpus and update Dataset Entry accordingly
	private int ingestFile(String folderName, File fileEntry, String dataProvider, String sourceID, String datasetType, String fileType, String startTimestamp, String endTimestamp, String fps) {
		System.out.println("*Ingesting file: "+fileEntry.getName());
		
		//
		//		CALL APIs to ingest file in the Corpus HDFS
		// Upload file and get 'Datanode_HDFS' and 'Url_HDFS' values
		// 					PUT CODE HERE
		// UPDATE THE METHOD -> 'sendFileToHDFS'
		if(ingestedSnippets==0) {
			if(datasetID==null) {
				datasetID="MARVEL."+dataProvider+"_"+sourceID+"."+datasetType+".N.N.OrigninalData."+TimestampToDate(startTimestamp)+"_1";
			}
			datasetIds.add(datasetID);
		}
		String Datanode_HDFS = "datafromapp";
		String Url_HDFS = datasetID+"__"+(ingestedSnippets+1);
		
			//Perform the ingestion in Dataset Entry (create a new one if it does not exist)
		Boolean isIngested = ingestSnippetToDataset(fileEntry.getName(), sourceID, startTimestamp, endTimestamp, datasetType, fileType, Datanode_HDFS, Url_HDFS, fps);
		if(isIngested==false) {
			System.err.println("\tError in searching, creating, or appending dataset entry");
			return(-2);
		}
		String newDatasetID=Url_HDFS;	// or just 'datasetID'
		return(sendFileToHDFS(folderName,fileEntry.getName(),newDatasetID,DatasetJSON.toString()));
		//if -1, file not sent
	}
	
		//Find the related dataset (create a one if it does not exist) and ingest file
	private Boolean ingestSnippetToDataset(String fileName, String sourceID, String startTimestamp, String endTimestamp, String datasetType, String fileType, String Datanode_HDFS, String Url_HDFS, String fps) {		
		if(ingestedSnippets==0) { //if this is the first snippet in the dataset, then set dataset's UUID and ID
			UUID uuid = UUID.randomUUID();
			String uuidAsString = uuid.toString();
			try {
				DatasetJSON.put("key",uuidAsString);
				DatasetJSON.put("datasetID",datasetID); //XX insert dataset ID
				JSONObject jbDatasetInfo = DatasetJSON.getJSONObject("datasetInfo");
				jbDatasetInfo.put("dataset",datasetID);
				
				JSONObject jbDataType = DatasetJSON.getJSONObject(datasetType.toLowerCase());
				jbDataType.put("deviceId",sourceID);
				if(datasetType.equalsIgnoreCase("video") || datasetType.equalsIgnoreCase("audio-video"))
					jbDataType.put("videoFps",fps);
			}
			catch(Exception e) {
				System.out.println("\tError in assigning UUID and DatasetID for new dataset.\n"+e.toString());
			}
		}
			//APPEND DATASET
		appendSnippet(fileName, datasetID, datasetType, fileType, sourceID, startTimestamp, endTimestamp, Datanode_HDFS, Url_HDFS);
		return(true);
	}
	
		// Get the Date from a Timestamp
	private String TimestampToDate(String timestamp) {
		Date testDate=null;
		try {
			testDate=new SimpleDateFormat("yyyyMMdd'T'HHmmss").parse(timestamp);
		}
		catch(Exception e) {
			System.out.println("\tError in checking timestamp. "+e);
		}
		
		return(new SimpleDateFormat("dd-MM-yyyy").format(testDate));
	}

		// Read the JSON template of Dataset entries from a file
	private JSONObject readDatasetTemplateFromFile(String fileName) {
		String content = "";
		JSONObject jbTemplate;
		
		try {
			File file = new File(fileName);
			BufferedReader reader = new BufferedReader(new FileReader (file));
		    String line = null;
		    while((line = reader.readLine()) != null) {
		    	content+=line;
		    }
		    reader.close();
		}
		catch(Exception e) {
			System.out.println("\tError while parsing JSON template from file: "+e);
			return(null);
		}
		
		try {
			jbTemplate = new JSONObject(content);
		}
		catch(Exception e) {
			System.out.println("\tError while converting JSON template from file");
			return(null);
		}
		return(jbTemplate);
	}
	
		// Update Snippet JSON and append it in a Dataset JSON
	private int appendSnippet(String fileName, String datasetID, String datasetType, String fileType, String sourceID, String startTimestamp, String endTimestamp, String Datanode_HDFS, String Url_HDFS) {
			// Append snippet in dataset
		//JSONObject jbDataset = getDatasetJSON(datasetID);
		JSONObject jbDataset=DatasetJSON;
		Boolean updateExistingDataset=false;
		try {
			JSONObject jbMetaData = jbDataset.getJSONObject(datasetType.toLowerCase());
			JSONArray snippetArray = jbMetaData.getJSONArray("snippets");
				// CHECK if the SNIPPET already exists
			//JSONObject jbSnippet = seachAndUpdateIfSnippetExists(snippetArray, startTimestamp, endTimestamp, Datanode_HDFS, Url_HDFS, fileName, fileType);
			
			Date tmpDate=null;
			try {	// MT timestamp format
				tmpDate=new SimpleDateFormat("yyyyMMdd'T'HHmmss").parse(startTimestamp);
			}	
			catch(Exception e) {
				//System.out.println("\tWarning in converting timestamp. "+e);
				try {	// General timestamp format
					tmpDate = new Date(Long.parseLong(startTimestamp));
				}
				catch(Exception e2) {
					System.out.println("\tError in converting timestamp. "+e2);
				}
			}
			
			try {
				for(int i=0;i<snippetArray.length();i++) {
					if(Long.parseLong(snippetArray.getJSONObject(i).getString("starts")) == tmpDate.getTime() ) {
						// Update existing snippet entry
						if(fileType.equals("txt") || fileType.equals("xml") || fileType.equals("eaf") || fileType.equals("zip") || fileType.equals("7z"))
							if(fileType.equals("zip") && isAugmented==true)
								snippetArray.getJSONObject(i).put("snippetFileName", fileName);
							else
								snippetArray.getJSONObject(i).put("annotationFileName", fileName);
						else if(fileType.equals("scoring"))
							snippetArray.getJSONObject(i).put("inferenceResultsFileName", fileName);
						else 
							snippetArray.getJSONObject(i).put("snippetFileName", fileName);
						updateExistingDataset=true;
					}
				}
			}
			catch(Exception e) {
				System.out.println("\tError while traversing snippet array: "+e);
			}
			
			if(updateExistingDataset==false) {	// NEW SNIPPET
				JSONObject jbSnippet = createNewSnippet(startTimestamp, endTimestamp, Datanode_HDFS, Url_HDFS, fileName, fileType);
				snippetArray.put(jbSnippet);
				ingestedSnippets++;
			}
		}
		catch(Exception e) {
			System.out.println("\tError in appending snippet in dataset: "+e);
		}
		DatasetJSON=jbDataset;
		return(-1);
	}
	
		// Create a new Snippet Entry with Default values
	private JSONObject createNewSnippet(String startTimestamp, String endTimestamp, String Datanode_HDFS, String Url_HDFS, String fileName, String fileType) {
		JSONObject jbSnippet=null;
		try {
			jbSnippet = new JSONObject(SnippetJSONTemplate);
			
			UUID uuid = UUID.randomUUID();
			String uuidAsString = uuid.toString();
			jbSnippet.put("key", uuidAsString);
			
			Date publicationDate=null;
			Date publicationDate2=null;
			try {
				
				if(tokenizationType.equals("GRN")) {
					publicationDate = new Date(Long.parseLong(startTimestamp));
					publicationDate2 = new Date(Long.parseLong(endTimestamp));
				}
				else {
					// MT timestamp format
					int month = Integer.parseInt(startTimestamp.substring(4, 6));	// find month
					if(month<=12) {
						publicationDate=new SimpleDateFormat("yyyyMMdd'T'HHmmss").parse(startTimestamp);
						publicationDate2=new SimpleDateFormat("yyyyMMdd'T'HHmmss").parse(endTimestamp);
					}
					else {
						publicationDate=new SimpleDateFormat("yyyyddMM'T'HHmmss").parse(startTimestamp);
						publicationDate2=new SimpleDateFormat("yyyyddMM'T'HHmmss").parse(endTimestamp);
					}
				}
			}
			catch(Exception e) {
				System.out.println("\tWarning in converting timestamp. "+e+"\n\tTrying another format...");
				try {	// General timestamp format
					publicationDate = new Date(Long.parseLong(startTimestamp)*1000);	// epoch * 1000
					publicationDate2 = new Date(Long.parseLong(endTimestamp)*1000);
				}
				catch(Exception e2) {
					System.out.println("\tError in converting timestamp. "+e2);
				}
			}
			
			jbSnippet.put("publicationDate", (new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").format(publicationDate)).toString());
			jbSnippet.put("duration", Long.toString(publicationDate2.getTime()-publicationDate.getTime()));
			jbSnippet.put("starts", Long.toString(publicationDate.getTime()));
			jbSnippet.put("ends", Long.toString(publicationDate2.getTime()));
			jbSnippet.put("timestamp", Long.toString(System.currentTimeMillis()));
			jbSnippet.put("datanodeHDFS", Datanode_HDFS);
			jbSnippet.put("urlHDFS", Url_HDFS);
			
			if(fileType.equals("txt") || fileType.equals("xml") || fileType.equals("eaf") || fileType.equals("zip") || fileType.equals("7z"))
				if(fileType.equals("zip") && isAugmented==true)
					jbSnippet.put("snippetFileName", fileName);
				else
					jbSnippet.put("annotationFileName", fileName);
			else if(fileType.equals("scoring"))
				jbSnippet.put("inferenceResultsFileName", fileName);
			else 
				jbSnippet.put("snippetFileName", fileName);
			
		}
		catch(Exception e) {
			System.out.println("\tError in assigning the fields for a new snippet.\n"+e.toString());
		}
		return(jbSnippet);
	}
	
	
	// Delete Snippet JSON and update the Dataset JSON
	private int deleteSnippet(String fileName, String datasetType) {
		JSONObject jbDataset=DatasetJSON;
		try {
			JSONObject jbMetaData = jbDataset.getJSONObject(datasetType.toLowerCase());
			JSONArray snippetArray = jbMetaData.getJSONArray("snippets");
			
				// CHECK if the SNIPPET already exists
			//JSONObject jbSnippet = seachAndUpdateIfSnippetExists(snippetArray, startTimestamp, endTimestamp, Datanode_HDFS, Url_HDFS, fileName, fileType);
						
			try {
				for(int i=0;i<snippetArray.length();i++) {
					if(snippetArray.getJSONObject(i).getString("snippetFileName")==fileName) {
						// Delete existing snippet entry
						snippetArray.remove(i);
						ingestedSnippets--;
					}
				}
			}
			catch(Exception e) {
				System.out.println("\tError while deleting element from snippet array: "+e);
			}
		}
		catch(Exception e) {
			System.out.println("\tError in deleting snippet from dataset: "+e);
		}
		DatasetJSON=jbDataset;
		return(-1);
	}
	
	
					// CORPUS APIs
		//Method to summarize the details of the datasets. The result is saved on the file 'MARVEL_Corpus_Datasets.txt'
	public void surveyDatasets() {
		System.out.println("\t\t--Survey Corpus Datasets--");
		try {
			String command = "curl -u "+username+":"+pass+" "+CorpusURL+"info";
			ProcessBuilder processBuilder = new ProcessBuilder(command.split(" "));
			Process process = processBuilder.start();
			InputStream inputStream = process.getInputStream();
			InputStreamReader isr = new InputStreamReader(inputStream,
                    StandardCharsets.UTF_8);
			BufferedReader br = new BufferedReader(isr);
			String strCurrentLine;
	        if ((strCurrentLine = br.readLine()) == null) {
	        	System.out.println("No datasets");
	        	br.close();
	            isr.close();
	            inputStream.close();
	            process.destroy();
	            return;
	        }
			br.close();
            isr.close();
            inputStream.close();
            process.destroy();
			
            File file = new File("MARVEL_Corpus_Datasets.txt");
			FileWriter fr = new FileWriter(file, false);
			BufferedWriter br3 = new BufferedWriter(fr);
			PrintWriter pr = new PrintWriter(br3);
			pr.println("Number\tDataset ID\tDataset Key\tStatus\tProvider\tUse Case\tCategory\tisAnonymized\tisAnnotated\tisAugmented\tCounter");
            JSONArray json = new JSONArray(strCurrentLine);
            for (int i = 0; i < json.length(); i++) {
            	String datasetID, datasetKey, status, provider, useCase, category, isAnonymized, isAnnotated, isAugmented;
            	try {
            		datasetID = ((JSONObject) json.get(i)).getString("dataset");
                    datasetKey = ((JSONObject) json.get(i)).getString("key");
                    status = ((JSONObject) json.get(i)).getString("status");
                    provider = ((JSONObject) json.get(i)).getString("provider");
                    useCase = ((JSONObject) json.get(i)).getString("useCase");
                    category = ((JSONObject) json.get(i)).getString("category");
                    isAnonymized ="false";
                    isAnnotated ="false";
                    isAugmented ="false";
            	}
                catch(Exception e) {
                	System.out.println("\tError: The dataset:"+json.get(i));
                	System.out.println(e);
                	continue;
                }
                
                ////
                String command5 = "curl -u "+username+":"+pass+" "+CorpusURL+"marvel/v1/datasets/"+datasetKey;
    			ProcessBuilder processBuilder5 = new ProcessBuilder(command5.split(" "));
    			Process process5 = processBuilder5.start();
    			InputStream inputStream5 = process5.getInputStream();
    			InputStreamReader isr5 = new InputStreamReader(inputStream5,
                        StandardCharsets.UTF_8);
    			BufferedReader br5 = new BufferedReader(isr5);
                	// Receive response and retrieve Dataset's JSON
    			System.out.println("\tDataset Entry: ");
                String strCurrentLine5;
                if ((strCurrentLine5 = br5.readLine()) != null) {
                    JSONArray jsonArr5 = new JSONArray(strCurrentLine5);
                    try {
                    	JSONObject tmpjsonDataset = (JSONObject) jsonArr5.get(0);
                    	isAnonymized=getAttribute(tmpjsonDataset,"isAnonymized");
                    	isAnnotated=getAttribute(tmpjsonDataset,"isAnnotated");
                    	isAugmented=getAttribute(tmpjsonDataset,"isAugmented");
                    }
                    catch(Exception e) {
                    	System.out.println("\tError: The dataset ("+datasetKey+") does not exist");
                    	System.out.println(e);
                    }
                }
                
                br5.close();
                isr5.close();
                inputStream5.close();
                process5.destroy();
                ////
                
                int counter=0;
                try {
                	//Get Snippets
                    String command2 = "curl -u "+username+":"+pass+" "+CorpusURL+"marvel/v1/snippetsofdataset/"+datasetKey;
            		ProcessBuilder processBuilder2 = new ProcessBuilder(command2.split(" "));
            		Process process2 = processBuilder2.start();
            		InputStream inputStream2 = process2.getInputStream();
            		InputStreamReader isr2 = new InputStreamReader(inputStream2,StandardCharsets.UTF_8);
            		BufferedReader br2 = new BufferedReader(isr2);
                    String strCurrentLine2;
                    if ((strCurrentLine2 = br2.readLine()) != null) {
                    	JSONArray jsonArr2 = new JSONArray(strCurrentLine2);
                    	counter=jsonArr2.length();
                    	System.out.println("datasetID: "+datasetID+" counter:"+ counter);
                    }
                        
                    br2.close();
                    isr2.close();
                    inputStream2.close();
                    process2.destroy();
                }
                catch(Exception e) {
                	System.out.println("\tError in surveying datasets -- count snippets");
            		System.out.println(e);
                }
                pr.println((i+1)+"\t"+datasetID+"\t"+datasetKey+"\t"+status+"\t"+provider+"\t"+useCase+"\t"+category+"\t"+isAnonymized+"\t"+isAnnotated+"\t"+isAugmented+"\t"+counter);
            }
            
            	// Close streams and processes
            pr.close();
			br3.close();
			fr.close();
		}
		catch(Exception e) {
			System.out.println("\tError in surveying datasets");
			System.out.println(e);
		}
	}
	
	
		// Call Corpus APIs to retrieve the current Dataset IDs
	private void readDatasetIDsFromCorpus() {
		try {
			String command = "curl -u "+username+":"+pass+" "+CorpusURL+"info";
			ProcessBuilder processBuilder = new ProcessBuilder(command.split(" "));
			Process process = processBuilder.start();
			InputStream inputStream = process.getInputStream();
			InputStreamReader isr = new InputStreamReader(inputStream,
                    StandardCharsets.UTF_8);
			BufferedReader br = new BufferedReader(isr);
			
            String strCurrentLine;
            
            if ((strCurrentLine = br.readLine()) != null) {
                JSONArray json = new JSONArray(strCurrentLine);
                for (int i = 0; i < json.length(); i++) {
                	String datasetID = ((JSONObject) json.get(i)).getString("dataset");
                	String datasetKey = ((JSONObject) json.get(i)).getString("key");
                	//System.out.println("Dataset["+i+"]="+datasetID);
                	datasetIds.add(datasetID);
                	datasetKeys.add(datasetKey);
                }
            }
            
            	// Close streams and processes
            br.close();
            isr.close();
            inputStream.close();
            process.destroy();
		}
		catch(Exception e) {
			System.out.println("\tError in calling curl for dataset IDs");
			System.out.println(e);
		}
	}
	
	
	// Call Corpus APIs to send a file to Corpus HDFS
	private int sendFileToHDFS(String folderName, String fileName, String datasetID, String metadata) {
		MediaType mediaType = MediaType.parse("text/plain");
		
		OkHttpClient.Builder builder = new OkHttpClient.Builder();
		builder.connectTimeout(120, TimeUnit.MINUTES); 
		builder.readTimeout(120, TimeUnit.MINUTES); 
		builder.writeTimeout(120, TimeUnit.MINUTES);
		OkHttpClient client = builder.build();
		
		RequestBody body = new MultipartBody.Builder().setType(MultipartBody.FORM)
				  .addFormDataPart("files",fileName,
				    RequestBody.create(new File(folderName+pathDash+fileName),MediaType.parse("application/octet-stream")))
				  .addFormDataPart("path","/"+datasetID)
				  .addFormDataPart("datasetname",datasetID)
				  .addFormDataPart("metadata",metadata)
				  .build();
		Builder bl = new Builder();
		bl.url(CorpusURL+"file").method("POST", body).addHeader("Accept", "application/json").addHeader("Authorization", "Basic Z2thbG9naWFubmlzOjJNRHJlN2FoZEZYUXlNeQ==").build();
		
		
		int tries=0;
		
		for(tries=0; tries<3; tries++) {
			Request request = new Request(bl.getUrl$okhttp(), bl.getMethod$okhttp(), bl.getHeaders$okhttp().build(), bl.getBody$okhttp(), bl.getTags$okhttp());
			try {
				System.out.println("Request Lenght: "+request.body().contentLength());
				Response response = client.newCall(request).execute();
				System.out.println("--Response: "+response.toString());
				System.out.println("--Response Message: "+response.message());
				response.body().close();
				response.close();
				if(response.toString().contains("code=50") || response.toString().contains("code=40")) {
					System.out.println("\t**** Trial["+(tries+1)+"] Warning -- File not send: "+fileName+" **** ");
					continue;
				}
				break;
			}
			catch(Exception e){
				System.out.println("\t**** Trial["+(tries+1)+"] Warning -- File not send: "+fileName+" **** "+e);
			}
		}
		
		client.dispatcher().executorService().shutdown();
		client.connectionPool().evictAll();
		if(tries==3) {
			System.out.println("\t****Vital Error while sending files to the Corpus ("+fileName+")");
			return(-1);
		}
		return(0);
	}
	
		// Not in USE
		// Call Corpus APIs to retrieve a Dataset Entry as a JSON
	public JSONObject getDatasetJSON(String datasetID) {
		// 		CALL Corpus APIs
		// GET JSON FROM Corpus Based on datasetID
		//
		JSONObject jsonDataset = null;
		try {
				// CALL API
			//String datasetKey = "a41247b7-d79b-4ac5-98e2-36d1a94c5f02";
			//datasetID=datasetKey;
			String command = "curl -u "+username+":"+pass+" "+CorpusURL+"marvel/v1/datasetsid/"+datasetID;
			ProcessBuilder processBuilder = new ProcessBuilder(command.split(" "));
			Process process = processBuilder.start();
			InputStream inputStream = process.getInputStream();
			InputStreamReader isr = new InputStreamReader(inputStream,
                    StandardCharsets.UTF_8);
			BufferedReader br = new BufferedReader(isr);
            	// Receive response and retrieve Dataset's JSON 
            String strCurrentLine;
            if ((strCurrentLine = br.readLine()) != null) {
                JSONArray jsonArr = new JSONArray(strCurrentLine);
                System.out.println("Result: "+jsonArr.toString());
                try {
                	jsonDataset = (JSONObject) jsonArr.get(0);
                	System.out.println("Dataset (JSON): "+jsonDataset.toString());
                }
                catch(Exception e) {
                	System.out.println("\tError: The dataset "+datasetID+" does not exist");
                	System.out.println(e);
                }
            }
            br.close();
            isr.close();
            inputStream.close();
            process.destroy();
		}
		catch(Exception e) {
			System.out.println("\tError in calling curl for dataset JSON");
			System.out.println(e);
		}
		return(jsonDataset);
	}
	
	// Call Corpus APIs to retrieve a Dataset Entry as a JSON
	public JSONObject getWholeDatasetJSON(String datasetKey, String relatedDatasetID) {
		// 		CALL Corpus APIs
		// GET JSON FROM Corpus Based on datasetID
		//
		JSONObject jsonDataset = new JSONObject(), tmpjsonDataset=null;
		try {
				// CALL API
			String command = "curl -u "+username+":"+pass+" "+CorpusURL+"marvel/v1/datasets/"+datasetKey;
			ProcessBuilder processBuilder = new ProcessBuilder(command.split(" "));
			Process process = processBuilder.start();
			InputStream inputStream = process.getInputStream();
			InputStreamReader isr = new InputStreamReader(inputStream,
                    StandardCharsets.UTF_8);
			BufferedReader br = new BufferedReader(isr);
            	// Receive response and retrieve Dataset's JSON
			System.out.println("\tDataset Entry: ");
            String strCurrentLine;
            if ((strCurrentLine = br.readLine()) != null) {
                JSONArray jsonArr = new JSONArray(strCurrentLine);
                System.out.println("Result: "+jsonArr.toString());
                try {
                	tmpjsonDataset = (JSONObject) jsonArr.get(0);
                	System.out.println("Dataset (JSON): "+tmpjsonDataset.toString());
                }
                catch(Exception e) {
                	System.out.println("\tError: The dataset ("+datasetKey+"||"+relatedDatasetID+") does not exist");
                	System.out.println(e);
                }
                
                jsonDataset.put("key", tmpjsonDataset.getString("key"));
                jsonDataset.put("datasetID", relatedDatasetID);
                	//DatasetInfo
                JSONObject jsonDatasetInfo = new JSONObject();
                jsonDatasetInfo.put("dataset", getAttribute(tmpjsonDataset,"dataset"));
                jsonDatasetInfo.put("category", getAttribute(tmpjsonDataset,"category"));
                jsonDatasetInfo.put("dataProvider", getAttribute(tmpjsonDataset,"provider"));
                jsonDatasetInfo.put("description", getAttribute(tmpjsonDataset,"description"));
                jsonDatasetInfo.put("useCase", getAttribute(tmpjsonDataset,"useCase"));
                jsonDatasetInfo.put("keywords", getAttribute(tmpjsonDataset,"keywords"));
                jsonDatasetInfo.put("status", getAttribute(tmpjsonDataset,"status"));
                jsonDataset.put("datasetInfo", jsonDatasetInfo);
                	//Dataset Type info (e.g., video, audio, or audio-video
                JSONObject jsonDatasetTypeInfo = new JSONObject();
                jsonDatasetTypeInfo.put("noOfChannels", getAttribute(tmpjsonDataset,"noOfChannels"));
                jsonDatasetTypeInfo.put("annotationSoftware", getAttribute(tmpjsonDataset,"annotationSoftware"));
                jsonDatasetTypeInfo.put("annotationOntology", getAttribute(tmpjsonDataset,"annotationOntology"));
                jsonDatasetTypeInfo.put("latitude", getAttribute(tmpjsonDataset,"latitude"));
                jsonDatasetTypeInfo.put("longitude", getAttribute(tmpjsonDataset,"longitude"));
                jsonDatasetTypeInfo.put("duration", getAttribute(tmpjsonDataset,"duration"));
                jsonDatasetTypeInfo.put("deviceId", getAttribute(tmpjsonDataset,"deviceId"));
                jsonDatasetTypeInfo.put("isAnnotated", getAttribute(tmpjsonDataset,"isAnnotated"));
                jsonDatasetTypeInfo.put("isAugmented", getAttribute(tmpjsonDataset,"isAugmented"));
                jsonDatasetTypeInfo.put("augmentationMethod", getAttribute(tmpjsonDataset,"augmentationMethod"));
                jsonDatasetTypeInfo.put("isAnonymized", getAttribute(tmpjsonDataset,"isAnonymized"));
                jsonDatasetTypeInfo.put("anonymizationMethod", getAttribute(tmpjsonDataset,"anonymizationMethod"));
                
                if(jsonDatasetInfo.getString("category").toLowerCase().equals("video") || jsonDatasetInfo.getString("category").toLowerCase().equals("audio-video")) {
	                jsonDatasetTypeInfo.put("videoResolution", getAttribute(tmpjsonDataset,"videoResolution"));
	                jsonDatasetTypeInfo.put("videoFps", getAttribute(tmpjsonDataset,"videoFps"));
                }
                if(jsonDatasetInfo.getString("category").toLowerCase().equals("audio") || jsonDatasetInfo.getString("category").toLowerCase().equals("audio-video")) {
                	jsonDatasetTypeInfo.put("audioBitrate", getAttribute(tmpjsonDataset,"audioBitrate"));
                	jsonDatasetTypeInfo.put("audioSampling", getAttribute(tmpjsonDataset,"audioSampling"));
                }
                
                jsonDataset.put(jsonDatasetInfo.getString("category").toLowerCase(), jsonDatasetTypeInfo);
                
            }
            
            br.close();
            isr.close();
            inputStream.close();
            process.destroy();
            
            //Get Snippets
            String command2 = "curl -u "+username+":"+pass+" "+CorpusURL+"marvel/v1/snippetsofdataset/"+datasetKey;
			ProcessBuilder processBuilder2 = new ProcessBuilder(command2.split(" "));
			Process process2 = processBuilder2.start();
			InputStream inputStream2 = process2.getInputStream();
			InputStreamReader isr2 = new InputStreamReader(inputStream2,
                    StandardCharsets.UTF_8);
			BufferedReader br2 = new BufferedReader(isr2);
            	// Receive response and retrieve Dataset's JSON 
            String strCurrentLine2;
            System.out.println("\tSnippets: ");
            if ((strCurrentLine2 = br2.readLine()) != null) {
                JSONArray jsonArr2 = new JSONArray(strCurrentLine2);
                for(int i=0;i<jsonArr2.length();i++) {
                	try {
                		jsonArr2.getJSONObject(i).put("key", jsonArr2.getJSONObject(i).getString("snippetID"));
                		jsonArr2.getJSONObject(i).remove("snippetID");
                	}
                	catch(Exception e) {
                		System.out.println("Error while trying to change field 'snippetID' into 'key'");
                	}
                }
                System.out.println("Snippets Result: "+jsonArr2.toString());                
                JSONObject jbDatasetInfo = jsonDataset.getJSONObject("datasetInfo");
    			String datasetType = jbDatasetInfo.getString("category");
    			JSONObject jbMetaData = jsonDataset.getJSONObject(datasetType.toLowerCase());
    			jbMetaData.put("snippets", jsonArr2);
            }
            
            br2.close();
            isr2.close();
            inputStream2.close();
            process2.destroy();
            System.out.println("Final Dataset (JSON): "+jsonDataset.toString());
		}
		catch(Exception e) {
			System.out.println("\tError in calling curl for dataset JSON");
			System.out.println(e);
		}
		
		return(jsonDataset);
	}
	
		// Check if attribute is 'NULL'
	private String getAttribute(JSONObject jbDataset, String key) {
		try {
			key = jbDataset.getString(key);
			return(key);
		}
		catch(Exception e) {
			return(" ");
		}
	}
	
		// Call Corpus APIs to delete a Dataset (both the JSON entry and the underlying data)
	public boolean deleteDataset(String key) {
		// 		CALL Corpus APIs
		// Delete Dataset Entry from the Corpus
		//
		try {
				// CALL API
			String command = "curl -u "+username+":"+pass+" "+CorpusURL+"marvel/v1/delete/"+key;
			ProcessBuilder processBuilder = new ProcessBuilder(command.split(" "));
			Process process = processBuilder.start();
			InputStream inputStream = process.getInputStream();
			InputStreamReader isr = new InputStreamReader(inputStream,
	                StandardCharsets.UTF_8);
			BufferedReader br = new BufferedReader(isr);
	        	// Receive response and retrieve Dataset's JSON 
	        String strCurrentLine;
	        if ((strCurrentLine = br.readLine()) != null && strCurrentLine!="") {
	        	br.close();
	            isr.close();
	            inputStream.close();
	            process.destroy();
	            return(true);	// Successful deletion of dataset
	        }
	        else {
	        	br.close();
	            isr.close();
	            inputStream.close();
	            process.destroy();
	            return(false);	// The dataset with 'key' was not deleted. Maybe the 'key' does not exist.
	        }
		}
		catch(Exception e) {
			System.out.println("\tError in calling curl for deleting dataset");
			System.out.println(e);
			return(false); // The dataset with 'key' was not deleted
		}
	}
	
		// For DEBUGGING - Convert String -> Long int (time in ms) -> Date
	private void checkTimestamp(String timestamp) {
		try {
			Date testDate=new SimpleDateFormat("yyyyMMdd'T'HHmmss").parse(timestamp);
			//System.out.println("Date="+testDate);
		}
		catch(Exception e) {
			System.out.println("\tError in checking timestamp. "+e);
		}
	}
	
		// Default Snippet JSON Template
	private String SnippetJSONTemplate = "{\r\n"
			+ "	\"key\": \"Will be filled automatically\",\r\n"
			+ "	\"publicationDate\": \"<Date of data creation>\",\r\n"
			+ "	\"duration\": \"<snippet duration in seconds>\",\r\n"
			+ "	\"starts\": \"<snippet starting point in seconds>\",\r\n"
			+ "	\"ends\": \"<snippet ending point in seconds>\",\r\n"
			+ "	\"timestamp\": \"<the timestamp of data ingestion>\",\r\n"
			+ "	\"datanodeHDFS\": \"Will be filled automatically, e.g., datafromapp\",\r\n"
			+ "	\"urlHDFS\": \"Will be filled automatically, e.g., MARVEL.GRN_36.Audio.N.N.OrigninalData.29-04-2022_9\",\r\n"
			+ " \"datasetID\": \"Will be filled automatically\",\r\n"
			+ "	\"snippetFileName\":\"\",\r\n"
			+ "	\"annotationFileName\":\"\",\r\n"
			+ "	\"inferenceResultsFileName\":\"\",\r\n"
			+ "	\"annotatorId\": \"List of annotators\",\r\n"
			+ "	\"annotationSummary\": \"<type of events that refer to the corresponding snippet, parse the annotation file -> to place here>\",\r\n"
			+ "	\"additionalEvents\": \"<extra annotation events>\"\r\n"
			+ "}";
}
