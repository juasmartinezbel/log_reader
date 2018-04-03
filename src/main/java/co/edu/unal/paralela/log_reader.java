package co.edu.unal.paralela;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.*;


/**
 *	1. TOP 10 las ips que más consultas hicieron en el proxy
 *	2. TOP 10 los servidores mas visitados
 *	3. TOP 10 de los servidores donde mas bytes se descargaron
 **/

public class log_reader {
	
	public static String fileName;
	public static FileReader file;
	public static BufferedReader in;
	public static HashMap<String,Long> ipSearch;
	public static HashMap<String,Long> ipVisited;
	public static HashMap<String,Long> ipBytes;
	public static long lineCounts;
	public static double parallelTime;
	public static double sequentialTime;
	public static final long INIT=0;
	public static final boolean PARALLEL=true;
	public static final boolean SEQUENTIAL=false;
	
	/**
	 * Comparator to calculate the top 10s
	 *
	 */
	public static class EntryComparator implements Comparator<Entry<String,Long>> {
	    @Override
	    public int compare(Entry<String, Long> o1, Entry<String, Long> o2) {
	        if (o1.getValue() < o2.getValue()) {
	            return 1;
	        } else if (o1.getValue() > o2.getValue()) {
	            return -1;
	        }
	        return 0;
	    }

	}
	
	/**
	 * Recursive action to use for parallel search
	 *
	 */
	private static class logsTopper extends RecursiveAction {

        private final long startIndexInclusive;
        private final long endIndexExclusive;
        
        public HashMap<String,Long> inipSearch;
        public HashMap<String,Long> inipVisited;
        public HashMap<String,Long> inipBytes;
        public boolean success;
        
        
        logsTopper(final long setStartIndexInclusive, final long setEndIndexExclusive) {
            this.startIndexInclusive = setStartIndexInclusive;
            this.endIndexExclusive = setEndIndexExclusive;
        }
        
        /**
         * Mixes the maps from left and right into one big map
         * @param myMaps
         */
        public boolean mixValues(LinkedList<HashMap<String,Long>> myMaps) {
        	int counter=0;
        	
        	for(HashMap<String,Long> hm : myMaps) {
        		long value;
        		String mainKey ="";
        		if(hm!=null) {
	        		if(counter%3==0) {
	        			for(String key : hm.keySet()) {	        				 
	        				if(!ipSearch.containsKey(key)) ipSearch.put(key, INIT);
	        				value=hm.get(key);
	                    	try {ipSearch.put(key, ipSearch.get(key)+value);}
	                    	catch (Exception e){return false;}
	                    	
	            		}
	        			
	        		}else if(counter%3==1) {
	        			for(String key : hm.keySet()) {
	        				if(!ipVisited.containsKey(key)) ipVisited.put(key, INIT);
	        				value=hm.get(key);
	                    	try {ipVisited.put(key, ipVisited.get(key)+value);
	                    	}catch (Exception e){return false;}
	            		}
	        			
	        		}else if(counter%3==2){
	        			for(String key : hm.keySet()) {
	        				if(!ipBytes.containsKey(key)) ipBytes.put(key, INIT);
	                    	value=hm.get(key);		   
	                    	try {ipBytes.put(key, ipBytes.get(key)+value);}
	                    	catch (Exception e){return false;}
	                    	
	                    
	            		}
	        			
	        		}
	        		
        		}
        		counter++;
        	}
        	
        	return true;
        }
        
        /*
		System.err.println("ipBytes=> "+key + " "+ ipBytes.containsKey(key));
		
		for(String s : ipBytes.keySet())
			System.out.println(s+": "+ipBytes.get(s));
		e.printStackTrace();
		System.exit(-2);*/
    
        static int SEQUENTIAL_THRESHOLD = 5;
        
        @Override
        protected void compute() {
        	
        	int hi = (int) endIndexExclusive;
        	int lo = (int) startIndexInclusive;
        	long len = lineCounts;
        	long mid = ((startIndexInclusive+endIndexExclusive)/2);
        	
        	if((endIndexExclusive - startIndexInclusive) <= (len/SEQUENTIAL_THRESHOLD)){
        		inipSearch=new HashMap<String,Long>();
        		inipVisited=new HashMap<String,Long>();
        		inipBytes=new HashMap<String,Long>();
        		try {
	        		for(long l=lo;l<hi;l++) {
		    			String line;
		    			line = in.readLine();
		    			if(line!=null) {
			    			ArrayList <String>lineSplit = new ArrayList<>(Arrays.asList(line.split(" ")));
			    			lineSplit.removeAll(Arrays.asList(null,""));
			    			String remotehost=lineSplit.get(2);
			    			String URL=lineSplit.get(6);
			    			long bytes=Long.valueOf(lineSplit.get(4));
			    			
			    			if(!inipSearch.containsKey(remotehost)) inipSearch.put(remotehost, INIT);
			    			if(!inipVisited.containsKey(URL)) inipVisited.put(URL, INIT);
			    			if(!inipBytes.containsKey(remotehost)) inipBytes.put(remotehost, INIT);
			
			    			inipSearch.put(remotehost, inipSearch.get(remotehost)+1);
			    			inipVisited.put(URL, inipVisited.get(URL)+1);
			    			inipBytes.put(remotehost, inipBytes.get(remotehost)+bytes);	
	        			}
	                }
        		} catch(Exception e) {
        			System.err.println(e);
        			System.exit(-2);
        		}
            }else{
            	logsTopper left = new logsTopper(startIndexInclusive, mid);
            	logsTopper right = new logsTopper(mid, endIndexExclusive);
                left.fork();
                right.compute();
                left.join();
                LinkedList<HashMap<String,Long>> myMaps = new LinkedList<HashMap<String,Long>>();
				myMaps.add(right.inipSearch);
				myMaps.add(right.inipVisited);
				myMaps.add(right.inipBytes);
				myMaps.add(left.inipSearch);
				myMaps.add(left.inipVisited);
				myMaps.add(left.inipBytes);
                success=mixValues(myMaps);
            }
        }
        
    }
	
	/**
	 * Prints the top 10s
	 * 
	 */
	public static String topResults() {
		List<Entry<String,Long>> mostIpSearch = new ArrayList<>(ipSearch.entrySet());
	    Collections.sort(mostIpSearch, new EntryComparator());
	    mostIpSearch=mostIpSearch.subList(0, 10);
	    
	    List<Entry<String,Long>> mostIpVisited = new ArrayList<>(ipVisited.entrySet());
	    Collections.sort(mostIpVisited, new EntryComparator());
	    mostIpVisited=mostIpVisited.subList(0, 10);
	    
	    List<Entry<String,Long>> mostBytes = new ArrayList<>(ipBytes.entrySet());
	    Collections.sort(mostBytes, new EntryComparator());
	    mostBytes=mostBytes.subList(0, 10);
	    
	    String tops = "";
	    String divide= "\n\n-------------------------------\n";
	    tops+=divide+"10 IPS QUE MÁS HICIERON CONSULTAS:\n\n";
	    int count=1;
	    for(Entry<String,Long> E : mostIpSearch) { 
	    	tops+=count+". \""+E.getKey()+"\" hizo un total de: "
	    						+E.getValue()+" consultas.\n";
	    	count++;
	    }
	    
	    tops+=divide+"10 IPS QUE MÁS SE VISITARON:\n\n";
	    count=1;
	    for(Entry<String,Long> E : mostIpVisited) { 
	    	tops+=count+". \""+E.getKey()+"\" recibió un total de: "
	    						+E.getValue()+" visitas.\n";
	    	count++;
	    }
	    
	    
	    tops+=divide+"10 IPS QUE MÁS DESCARGARON:\n\n";
	    count=1;
	    for(Entry<String,Long> E : mostBytes) { 
	    	tops+=count+". \""+E.getKey()+"\" descargó un total de: "
	    						+E.getValue()+" bytes.\n";
	    	count++;
	    }
	    
	    return tops;
	}
	
	
	
	/**
	 * Calculates the top 10s sequentially
	 * @throws IOException
	 */
	public static boolean sequentialTop() throws IOException {
		String line;
		while((line=in.readLine())!=null) {
			if(line!=null) {
				ArrayList <String>lineSplit = new ArrayList<>(Arrays.asList(line.split(" ")));
				lineSplit.removeAll(Arrays.asList(null,""));
				
				String remotehost=lineSplit.get(2);
				String URL=lineSplit.get(6);
				long bytes=Long.valueOf(lineSplit.get(4));
				
				if(!ipSearch.containsKey(remotehost)) ipSearch.put(remotehost, INIT);
				if(!ipVisited.containsKey(URL)) ipVisited.put(URL, INIT);
				if(!ipBytes.containsKey(remotehost)) ipBytes.put(remotehost, INIT);
				
				ipSearch.put(remotehost, ipSearch.get(remotehost)+1);
				ipVisited.put(URL, ipVisited.get(URL)+1);
				ipBytes.put(remotehost, ipBytes.get(remotehost)+bytes);
			}	

		}
		return true;
			
	}
	
	/**
	 * Calculates the Top 10s in parallel 
	 * 
	 */
	public static boolean parallelTop() throws IOException {
		logsTopper t = new logsTopper(0,lineCounts);
		t.compute();
		boolean results = t.success ? true : false;
		return results;
	}


	

    
	public static long fileSize() throws IOException {
		FileReader input = new FileReader(fileName);
		LineNumberReader count = new LineNumberReader(input);
		while (count.skip(Long.MAX_VALUE) > 0){}
		return count.getLineNumber() + 1;    
	}
    
	/**
	 * Initializes the file to be read
	 * 
	 */
	public static void initializeFile() throws IOException {
		file = new FileReader(fileName);
		in = new BufferedReader (file);
	}
	
	public static String compute_logs(boolean is_parallel) throws IOException {
		initializeFile();
		ipSearch=new HashMap<String,Long>();
		ipVisited=new HashMap<String,Long>();
		ipBytes=new HashMap<String,Long>();
		
		String total="";
		String type = is_parallel ? "paralelo" : "secuencial";
		String stars="************";
		String [] marc = 
				{"**********************************", 
				stars+type.toUpperCase()+stars};
		marc[1]= is_parallel ? "*"+marc[1]+"*":marc[1];
		
		total+=marc[0]+"\n"+marc[1]+"\n"+marc[0];
		
		String results;
		boolean success=false;
		long StartTime=0;
		long StopTime=0;
		
		while(!success) {
			StartTime = System.nanoTime();
			success = is_parallel ? parallelTop() : sequentialTop();
			StopTime = System.nanoTime();
		}
		results=topResults();
		
		double totalTime=(StopTime-StartTime)/1000000000.0;
		if(is_parallel) parallelTime= totalTime;
		else sequentialTime = totalTime;
		
		total += "\n"+results;
		total += "\n\n"+"El "+type+" se demoró: "+totalTime+"s";
		return total;
	}
	
	
	public static void main(String[] args) throws IOException {	
		System.out.println("Calculando...");
		
		//Change the name of the file down hear to start your program
		fileName="access.log";
		lineCounts = fileSize();
        PrintStream P = new PrintStream(new File("Paralelo.txt"));
        PrintStream S = new PrintStream(new File("Secuencial.txt"));
        PrintStream console = System.out;
 
        System.setOut(S);
		System.out.println(compute_logs(SEQUENTIAL));
		System.setOut(P);
		System.out.println(compute_logs(PARALLEL));
		
		System.setOut(console);
		double totalTime=sequentialTime/parallelTime;
		System.out.println("\n\nLa razón del tiempo del paralelo con respecto al secuencial fue de: "+totalTime);
		
		in.close ();
	}

}
