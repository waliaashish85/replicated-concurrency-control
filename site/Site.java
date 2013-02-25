package site;
import java.io.*;
import java.util.*;

/**
 * A place holder representing site.
 * @author Ashish Walia
 *
 */
public class Site {
	private boolean isSiteUp;
	private int siteId;
	private int siteFailedAtTick;
	private Map mapOfVariablesAndValues = null;
	public Site(int siteId, Map mapOfVariablesAndValues)
	{	
		isSiteUp = true;
		this.siteId=siteId;
		this.mapOfVariablesAndValues = new HashMap();
		//Deep cloning not required
		this.mapOfVariablesAndValues = mapOfVariablesAndValues;
	}
		
	/**
	 * A method which fails a site by setting the isSiteUp flag as false
	 * @author Ashish Walia
	 * @param tick - An integer value which helps in maintaining the time stamp information when site fails
	 */
	public void failSite(int tick)
	{
		isSiteUp=false;
		siteFailedAtTick=tick;
	}
	/**
	 * A method which recovers a failed site by setting the isSiteUp flag as true
	 * @author Ashish Walia
	 */
	public void recoverSite()
	{
		isSiteUp=true;
	}
	public int getTickWhenSiteFailed()
	{
		return siteFailedAtTick;
	}
	public boolean getSiteStatus()
	{
		return isSiteUp;
	}
	public int getSiteId()
	{
		return siteId;
	}
	public Map getMapOfVariablesAndValuesAtSite()
	{
		return mapOfVariablesAndValues;
	}
	/**
	 * @param variable
	 * @return
	 */
	public boolean isVariableAvailableOnSite(String variable)
	{
		if(variable!=null && !"".equals(variable) && isSiteUp && mapOfVariablesAndValues!=null && mapOfVariablesAndValues.containsKey(variable))
		{
			return true;
		}else
		{
			return false;
		}
	}
	/**
	 * @param variable
	 * @return
	 */
	public int getValueOfVariableAtSite(String variable)
	{
		if(variable!=null && !"".equals(variable) && isSiteUp && mapOfVariablesAndValues!=null && mapOfVariablesAndValues.containsKey(variable))
		{
			VariablesVO variableVO = (VariablesVO)mapOfVariablesAndValues.get(variable);
			if(variableVO!=null)
			{
				return variableVO.getValue();
			}
			else
			{
				return 0;
			}
		}else
		{
			return 0;
		}
	}
	/**
	 * Method which dumps committed values of the variable available at this site to console and output  file.
	 * @author - Ashish Walia
	 * @param out - A BufferedWriter object to write contents in the output file
	 * @param variable - A string variable whose committed value needs to be dumped on the console and the output file
	 * @throws IOException
	 */
	public void dump(String variable, BufferedWriter out)throws IOException
	{
		if(isVariableAvailableOnSite(variable))
		{
			out.write("----------");
			out.newLine();
			out.write("Site "+ siteId);
			out.newLine();
			out.write("----------");
			out.newLine();
			out.write(variable+"="+getValueOfVariableAtSite(variable));
			out.newLine();
			System.out.println("----------");
			System.out.println("Site "+ siteId);
			System.out.println("----------");			
			System.out.println(variable+"="+getValueOfVariableAtSite(variable));	
		}
	}
	/**
	 * A method to check if the variable is available to be read from this site or not.
	 * @auhor Ashish Walia
	 * @param variable - A string variable which needs to be checked for if it is available for read operation or not
	 * @return
	 */
	public boolean isVariableAvailableToBeReadFromSite(String variable)
	{
		if(variable!=null &&!"".equals(variable) && getSiteStatus()&& getMapOfVariablesAndValuesAtSite()!=null && getMapOfVariablesAndValuesAtSite().containsKey(variable))
		{
			VariablesVO variableVO = (VariablesVO)getMapOfVariablesAndValuesAtSite().get(variable);
			if(variableVO!=null && variableVO.isVariableAvailableForReadOperation())
			{
				return true;
			}
			else
			{
				return false;
			}
		}
		else
		{
			return false;
		}
	}
	/**
	 * Method which dumps committed values of all the variables available at this site to console and output  file
	 * @author - Ashish Walia
	 * @param out - A BufferedWriter object to write contents in the output file
	 * @throws IOException
	 */
	public void dump(BufferedWriter out) throws IOException
	{
		if(isSiteUp && mapOfVariablesAndValues!=null)
		{
			out.write("----------");
			out.newLine();
			out.write("Site "+ siteId);
			out.newLine();
			out.write("----------");
			out.newLine();
			System.out.println("----------");
			System.out.println("Site "+ siteId);
			System.out.println("----------");
			List <VariablesVO>list = new ArrayList<VariablesVO>();
			Iterator itr =(Iterator)mapOfVariablesAndValues.entrySet().iterator();
			while(itr.hasNext())
			{
				Map.Entry pairs = (Map.Entry)itr.next();
				if(pairs!=null)
				{
					VariablesVO variableVO=(VariablesVO)pairs.getValue();
					if(variableVO!=null)
					{
						list.add(variableVO);
					}
				}
			}
			Collections.sort(list);
			Iterator listIterator = (Iterator)list.iterator();
			while(listIterator.hasNext())
			{
				VariablesVO variableVO=(VariablesVO)listIterator.next();
				if(variableVO!=null && variableVO.getVariable()!=null && !"".equals(variableVO.getVariable()))
				{
					out.write(variableVO.getVariable()+"="+variableVO.getValue() + "   ");
					System.out.print(variableVO.getVariable()+"="+variableVO.getValue() + "   ");
				}
			}
			out.newLine();
			System.out.println("");	
			
		}
	}
	
	
	
}
