import java.util.*;
import java.io.*;
import site.DataAndLockManager;
import site.Site;
import transaction.TransactionManager;
import operation.OperationVO;
/**
 * Main driver class which takes input file with full path as command line argument, parses the file, initializes sites, data and lock manager and transaction manager,
 * triggers execution of operations and writes output to out.txt file
 * @author Ashish Walia
 *
 */
public class RepCRec {
	
	public static void main(String[] args) {		
		
		//Map of data lock managers
		Map dataAndLockManagersMap = new HashMap();
		//Map which holds all the site objects		
		Map sites = new HashMap();
		//Set
		Set variables = new HashSet();
		
		String variablesAvailableAtAllSites[]={"x2","x4","x6","x8","x10","x12","x14","x16","x18","x20"};
		int valueofVariablesAvailableAtAllSites[]={20,40,60,80,100,120,140,160,180,200};
		String uncommonVariablesAtSite2[]={"x1","x11"};
		int valueOfUncommonVariablesAtSite2[]={10,110};
		String uncommonVariablesAtSite4[]={"x3","x13"};
		int valueOfUncommonVariablesAtSite4[]={30,130};
		String uncommonVariablesAtSite6[]={"x5","x15"};
		int valueOfUncommonVariablesAtSite6[]={50,150};
		String uncommonVariablesAtSite8[]={"x7","x17"};
		int valueOfUncommonVariablesAtSite8[]={70,170};
		String uncommonVariablesAtSite10[]={"x9","x19"};
		int valueOfUncommonVariablesAtSite10[]={90,190};
		
		RepCRecHelper helper = new RepCRecHelper();
		List operations = new ArrayList();
		if(args.length>0)
		{
			try
			{		
				//Mention full qualified path with the file name
				BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(args[0])));
				//BufferedReader bufferedReader = new BufferedReader(new FileReader("C:\\Ashish\\RepCRec\\src\\Input.txt"));
				helper.parseInputFile(bufferedReader, operations);
				
			}			
			catch(FileNotFoundException fileNotFoundException)
			{
				System.out.println("File not found.");
			}
			catch(IOException ioException)
			{
				System.out.println("Unable to read input file.");
			}
		}
		else
		{
			System.out.println("Please provide input file as a command line arguement.");
		}
		
		//Instantiating sites. We don't want any common or uncommon variable at different sites to share the same address space
		for(int i=1;i<=10;i++)
		{
			//Instantiate data and lock manager for each site			
			Set variablesAtSite = new HashSet();
			Set variableVOAtSite = new HashSet();
			Map mapOfVariablesAndValues = new HashMap();			
			
			//Treatment for common variables
			helper.initializeSite(variablesAvailableAtAllSites, valueofVariablesAvailableAtAllSites, mapOfVariablesAndValues);
			if(i==2)
			{
				helper.initializeSite(uncommonVariablesAtSite2, valueOfUncommonVariablesAtSite2, mapOfVariablesAndValues);
			}
			else if(i==4)
			{				
				helper.initializeSite(uncommonVariablesAtSite4, valueOfUncommonVariablesAtSite4, mapOfVariablesAndValues);
			}
			else if(i==6)
			{
				helper.initializeSite(uncommonVariablesAtSite6, valueOfUncommonVariablesAtSite6, mapOfVariablesAndValues);
			}
			else if(i==8)
			{				
				helper.initializeSite(uncommonVariablesAtSite8, valueOfUncommonVariablesAtSite8, mapOfVariablesAndValues);
			}
			else if(i==10)
			{
				helper.initializeSite(uncommonVariablesAtSite10, valueOfUncommonVariablesAtSite10, mapOfVariablesAndValues);
			}
			Site site = new Site(i,mapOfVariablesAndValues);
			DataAndLockManager dataAndLockManager = new DataAndLockManager(i,site);
			dataAndLockManagersMap.put(i, dataAndLockManager);				
		}
		//Instantiate transaction manager
		TransactionManager transactionManager = new TransactionManager(dataAndLockManagersMap);
		BufferedWriter out =null;
		try
		{		
			
			out = new BufferedWriter(new FileWriter("out.txt"));			
			helper.executeOperations(transactionManager, dataAndLockManagersMap, operations,out);	
			out.close();
		}			
		
		catch(IOException ioException)
		{
			System.out.println("Unable to create output file.");			
		}
		
		
		
	}

}
