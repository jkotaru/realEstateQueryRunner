/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package queryrunner;

import java.util.ArrayList;
import java.util.Scanner;

/**
 * 
 * QueryRunner takes a list of Queries that are initialized in it's constructor
 * and provides functions that will call the various functions in the QueryJDBC class 
 * which will enable MYSQL queries to be executed. It also has functions to provide the
 * returned data from the Queries. Currently the eventHandlers in QueryFrame call these
 * functions in order to run the Queries.
 */
public class QueryRunner {

    
    public QueryRunner()
    {
        this.m_jdbcData = new QueryJDBC();
        m_updateAmount = 0;
        m_queryArray = new ArrayList<>();
        m_error="";
    
        
        // TODO - You will need to change the queries below to match your queries.
        
        // You will need to put your Project Application in the below variable
        
        this.m_projectTeamApplication="REAL-ESTATE_MANAGEMENT";    // THIS NEEDS TO CHANGE FOR YOUR APPLICATION
        
        // Each row that is added to m_queryArray is a separate query. It does not work on Stored procedure calls.
        // The 'new' Java keyword is a way of initializing the data that will be added to QueryArray. Please do not change
        // Format for each row of m_queryArray is: (QueryText, ParamaterLabelArray[], LikeParameterArray[], IsItActionQuery, IsItParameterQuery)
        
        //    QueryText is a String that represents your query. It can be anything but Stored Procedure
        //    Parameter Label Array  (e.g. Put in null if there is no Parameters in your query, otherwise put in the Parameter Names)
        //    LikeParameter Array  is an array I regret having to add, but it is necessary to tell QueryRunner which parameter has a LIKE Clause. If you have no parameters, put in null. Otherwise put in false for parameters that don't use 'like' and true for ones that do.
        //    IsItActionQuery (e.g. Mark it true if it is, otherwise false)
        //    IsItParameterQuery (e.g.Mark it true if it is, otherwise false)
        
        // m_queryArray.add(new QueryData("Select * from contact", null, null, false, false));   // THIS NEEDS TO CHANGE FOR YOUR APPLICATION
        // m_queryArray.add(new QueryData("Select * from contact where contact_id=?", new String [] {"CONTACT_ID"}, new boolean [] {false},  false, true));        // THIS NEEDS TO CHANGE FOR YOUR APPLICATION
        // m_queryArray.add(new QueryData("Select * from contact where contact_name like ?", new String [] {"CONTACT_NAME"}, new boolean [] {true}, false, true));        // THIS NEEDS TO CHANGE FOR YOUR APPLICATION
        // m_queryArray.add(new QueryData("insert into contact (contact_id, contact_name, contact_salary) values (?,?,?)",new String [] {"CONTACT_ID", "CONTACT_NAME", "CONTACT_SALARY"}, new boolean [] {false, false, false}, true, true));// THIS NEEDS TO CHANGE FOR YOUR APPLICATION
        
        // Query 1  where STATE_NAME = "WA" or "OR"
        // Lists top 10 (based on price) properties in WA and OR states
        m_queryArray.add(new QueryData("SELECT A.ADDR_STR AS 'Street Address', A.ADDR_CITY AS 'City', A.ADDR_STATE AS 'State', A.ADDR_ZIP AS 'Zipcode', \n" +
                "P.PROP_BED AS 'Bedrooms', P.PROP_BATH AS 'Bathrooms', P.PROP_TARGET_PRICE AS 'Property Price'\n" +
                "FROM PROPERTY AS P\n" +
                "JOIN ADDRESS AS A ON A.ADDR_ID = P.ADDR_ID\n" +
                "JOIN DEAL AS D ON D.PROP_ID = P.PROP_ID\n" +
                "WHERE (A.ADDR_STATE =? OR A.ADDR_STATE =?)\n" +
                "AND P.PROP_BATH > 2\n" +
                "AND PROP_BATH > 1.5\n" +
                "ORDER BY P.PROP_TARGET_PRICE DESC\n" +
                "LIMIT 10;",new String [] {"STATE_NAME","STATE_NAME"},new boolean [] {false, false},false,true));

        // Query 2  where DEALSTATUS = "CLOSED"
        // Average salary, comission percent, commision of working employees who closed a deal
        m_queryArray.add(new QueryData("SELECT PS.POS_TYPE AS 'Employee Position', ROUND(AVG(E.EMPL_BASESALARY)) AS 'Avg Base Salary',\n" +
                "ROUND(AVG(E.EMPL_COMISSION), 3) AS 'Avg Comission %', ROUND(AVG(IT.Comission)) AS 'Avg Comission'\n" +
                "FROM EMPLOYEE AS E\n" +
                "JOIN \n" +
                "(SELECT D.EMPL_ID, SUM(E.EMPL_COMISSION * PRIC_PRICE) as 'Comission'\n" +
                "FROM DEAL AS D\n" +
                "JOIN EMPLOYEE E ON E.EMPL_ID = D.EMPL_ID\n" +
                "JOIN PRICE PR ON PR.DEAL_ID = D.DEAL_ID\n" +
                "WHERE D.DEAL_STATUS =?\n" +
                "GROUP BY D.EMPL_ID) AS IT ON IT.EMPL_ID = E.EMPL_ID\n" +
                "JOIN EMPL_POSITION AS EP ON EP.EMPL_ID = E.EMPL_ID\n" +
                "JOIN POSITION AS PS ON PS.POS_ID = EP.POS_ID\n" +
                "WHERE E.EMPL_ENDDATE IS NULL\n" +
                "GROUP BY PS.POS_TYPE\n" +
                "ORDER BY PS.POS_TYPE;",new String [] {"DEAL_STATUS"},new boolean [] {false},false,true));

        // Query 3  where EMPLOYEE_ID = 24 and APPOINTMENT_DATE = "2022-05-16"
        // Show appointments for a given date for a given employee
        m_queryArray.add(new QueryData("SELECT APP_DATE, P.PERS_FNAME AS 'CUSTOMER_FNAME', \n" +
                "P.PERS_LNAME AS 'CUSTOMER_LNAME', P.PERS_PHONE AS 'CUSTOMER_PHONE',\n" +
                "P.PERS_EMAIL AS 'CUSTOMER_EMAIL', APP_DESCRIPTION, APP_TYPE\n" +
                "FROM APPOINTMENT AS A \n" +
                "JOIN EMPLOYEE AS E \n" +
                "ON E.EMPL_ID = A.EMPL_ID\n" +
                "JOIN PERSON AS P \n" +
                "ON P.PERS_ID = A.PERS_ID\n" +
                "WHERE E.EMPL_ID =? AND DATE(A.APP_DATE) =?\n" +
                "ORDER BY APP_DATE;",new String [] {"EMPLOYEE_ID","APPOINTMENT_DATE"},new boolean [] {false, false},false,true));

        // Query 4
        // PROPERTY WITH THE MAXIMUM SOLD PRICE
        m_queryArray.add(new QueryData("SELECT \n" +
                "PROPERTY.PROP_ID,\n" +
                "PROP_BED AS \"NUMBER OF BEDS\",\n" +
                "PROP_BATH \"NUMBER OF BATHS\",\n" +
                "ADDR_STR,\n" +
                "ADDR_ZIP,\n" +
                "ADDR_STATE,\n" +
                "ADDR_CITY,\n" +
                "PRIC_PRICE AS \"PRICE\"\n" +
                "FROM PROPERTY \n" +
                "JOIN (SELECT \n" +
                "PROP_ID, \n" +
                "PRIC_PRICE\n" +
                "FROM PRICE\n" +
                "WHERE PRIC_PRICE = (SELECT MAX(PRIC_PRICE) AS MAX_PRICE\n" +
                "FROM DEAL JOIN PRICE USING (PROP_ID)\n" +
                "WHERE DEAL_STATUS = \"CLOSED\" AND \n" +
                "DEAL_INSPECTION = \"Pass\" AND \n" +
                "(DEAL_FINANCING = \"Approved loan\" OR DEAL_FINANCING = \"Cash\"))) AS MAX_PRICE_DEAL \n" +
                "ON MAX_PRICE_DEAL.PROP_ID = PROPERTY.PROP_ID\n" +
                "JOIN ADDRESS USING (ADDR_ID);",null,null,false,false));

        // Query 5
        // PROPERTY WITH THE MINIMUM SOLD PRICE
        m_queryArray.add(new QueryData("SELECT \n" +
                "PROPERTY.PROP_ID,\n" +
                "PROP_BED AS \"NUMBER OF BEDS\",\n" +
                "PROP_BATH \"NUMBER OF BATHS\",\n" +
                "ADDR_STR,\n" +
                "ADDR_ZIP,\n" +
                "ADDR_STATE,\n" +
                "ADDR_CITY,\n" +
                "PRIC_PRICE AS \"PRICE\"\n" +
                "FROM PROPERTY \n" +
                "JOIN (SELECT \n" +
                "PROP_ID, \n" +
                "PRIC_PRICE\n" +
                "FROM PRICE\n" +
                "WHERE PRIC_PRICE = (SELECT MIN(PRIC_PRICE) AS MIN_PRICE\n" +
                "FROM DEAL JOIN PRICE USING (PROP_ID)\n" +
                "WHERE DEAL_STATUS = \"CLOSED\" AND \n" +
                "DEAL_INSPECTION = \"Pass\" AND \n" +
                "(DEAL_FINANCING = \"Approved loan\" OR DEAL_FINANCING = \"Cash\"))) AS MIN_PRICE_DEAL \n" +
                "ON MIN_PRICE_DEAL.PROP_ID = PROPERTY.PROP_ID\n" +
                "JOIN ADDRESS USING (ADDR_ID);",null,null,false,false));

        // Query 6  where PROPERTY_ID = 280
        // Show open deals for a given property sorted by price
        m_queryArray.add(new QueryData("SELECT LATEST_DEALS.DEAL_ID, P.PROP_ID, PRIC_PRICE, DEAL_STATUS, DEAL_INSPECTION, \n" +
                "DEAL_FINANCING, PERSON.PERS_FNAME, PERSON.PERS_LNAME, PERSON.PERS_PHONE\n" +
                "FROM\n" +
                "(SELECT PRICE.DEAL_ID, MAX(PRICE.PRIC_DATE) AS LATEST_PRICE\n" +
                "FROM \n" +
                "(SELECT DEAL_ID\n" +
                "FROM DEAL\n" +
                "WHERE PROP_ID =?) AS DEALS \n" +
                "JOIN PRICE \n" +
                "ON DEALS.DEAL_ID = PRICE.DEAL_ID\n" +
                "GROUP BY PRICE.DEAL_ID) AS LATEST_DEALS \n" +
                "JOIN DEAL AS D\n" +
                "ON LATEST_DEALS.DEAL_ID = D.DEAL_ID \n" +
                "JOIN PRICE AS P \n" +
                "ON LATEST_DEALS.DEAL_ID = P.DEAL_ID AND LATEST_DEALS.LATEST_PRICE = P.PRIC_DATE\n" +
                "JOIN PERSON \n" +
                "ON PERSON.PERS_ID = D.PERS_ID\n" +
                "ORDER BY P.PRIC_PRICE DESC;",new String [] {"PROPERTY_ID"},new boolean [] {false},false,true));

        // Query 7
        // Show sellers data who have listed 2 or more properties
        m_queryArray.add(new QueryData("SELECT P.PERS_FNAME AS FirstName, P.PERS_LNAME AS LastName,\n" +
                "A.ADDR_STR AS Street, A.ADDR_CITY AS City, A.ADDR_STATE AS State, A.ADDR_ZIP AS Zip,\n" +
                "P.PERS_PHONE AS Phone\n" +
                "FROM PERSON AS P\n" +
                "JOIN\n" +
                "(SELECT PERS_ID\n" +
                "FROM PROPERTY\n" +
                "GROUP BY PERS_ID\n" +
                "HAVING COUNT(*) >= 2) AS SUMP\n" +
                "ON P.PERS_ID = SUMP.PERS_ID\n" +
                "JOIN ADDRESS AS A\n" +
                "ON P.ADDRESS_ID = A.ADDR_ID;",null,null,false,false));

        // Query 8
        // Average salary, comission percent of all employees and comission for who closed a deal grouped by working status
        m_queryArray.add(new QueryData("SELECT PS.POS_TYPE AS 'Position', IF(E.EMPL_ENDDATE IS NULL, 'Yes', 'No') AS 'CurrWorking', \n" +
                "ROUND(AVG(E.EMPL_BASESALARY)) AS 'Average BaseSalary', ROUND(AVG(E.EMPL_COMISSION), 3) AS 'Comission Percent', \n" +
                "ROUND(AVG(OT.Comission)) AS 'Average Comission'\n" +
                "FROM EMPLOYEE AS E\n" +
                "JOIN EMPL_POSITION AS EP ON EP.EMPL_ID = E.EMPL_ID\n" +
                "JOIN POSITION AS PS ON PS.POS_ID = EP.POS_ID\n" +
                "LEFT OUTER JOIN \n" +
                "(SELECT D.EMPL_ID, SUM(E.EMPL_COMISSION * PRIC_PRICE) as 'Comission'\n" +
                "FROM DEAL AS D\n" +
                "JOIN EMPLOYEE E ON E.EMPL_ID = D.EMPL_ID\n" +
                "JOIN PRICE PR ON PR.DEAL_ID = D.DEAL_ID\n" +
                "WHERE D.DEAL_STATUS = 'CLOSED'\n" +
                "GROUP BY D.EMPL_ID) AS OT ON OT.EMPL_ID = E.EMPL_ID\n" +
                "GROUP BY CurrWorking, Position\n" +
                "ORDER BY CurrWorking DESC;",null,null,false,false));

        // Query 9
        // age range for our customers (min, avg, max)
        m_queryArray.add(new QueryData("SELECT MIN(AGE.age) AS MinAge, MAX(AGE.age) AS MaxAge, ROUND(AVG(AGE.age)) AS AverageAge\n" +
                "FROM (SELECT FLOOR(DATEDIFF(NOW(), PERS_DOB) / 365.2425) AS age\n" +
                "FROM PERSON\n" +
                "JOIN PERS_ROLE\n" +
                "ON PERSON.PERS_ID = PERS_ROLE.PERS_ID\n" +
                "JOIN ROLE\n" +
                "ON PERS_ROLE.ROLE_ID = ROLE.ROL_ID) AS AGE;",null,null,false,false));

        // Query 10  where NUM_OF_PROPERTIES = 10
        // Average closing price for different states, states where there are more than 10 properties listed
        m_queryArray.add(new QueryData("SELECT ADDR_STATE AS STATE, ROUND(AVG(PRIC_PRICE), 2) AS AveragePrice\n" +
                "FROM (SELECT DEAL.PROP_ID, PRIC_PRICE\n" +
                "FROM PRICE\n" +
                "JOIN DEAL USING (DEAL_ID)\n" +
                "WHERE DEAL_STATUS = 'CLOSED') AS PRICE\n" +
                "JOIN (SELECT PROPERTY.PROP_ID, ADDR_STATE\n" +
                "FROM PROPERTY \n" +
                "JOIN ADDRESS \n" +
                "ON PROPERTY.ADDR_ID = ADDRESS.ADDR_ID) AS STATE\n" +
                "ON PRICE.PROP_ID = STATE.PROP_ID\n" +
                "GROUP BY ADDR_STATE\n" +
                "HAVING COUNT(*) > ?;",new String [] {"NUM_OF_PROPERTIES"},new boolean [] {false},false,true));

        // Query 11
        // Average property closing price by number of bedrooms
        m_queryArray.add(new QueryData("SELECT PROP_BED AS Number_Of_Bedroom, ROUND (AVG(PRIC_PRICE), 2) AS Average_Price\n" +
                "FROM PROPERTY AS P\n" +
                "JOIN DEAL AS D\n" +
                "ON P.PROP_ID = D.PROP_ID\n" +
                "JOIN PRICE AS PRI\n" +
                "ON D.DEAL_ID = PRI.DEAL_ID\n" +
                "WHERE DEAL_STATUS = 'CLOSED'\n" +
                "GROUP BY PROP_BED\n" +
                "ORDER BY PROP_BED;",null,null,false,false));

        // Query 12
        // Top 3 employees by the sum of sales they made
        m_queryArray.add(new QueryData("SELECT E.EMPL_ID, P.PERS_FNAME AS First_Name, P.PERS_LNAME AS Last_Name,\n" +
                "TOTAL, E.EMPL_COMISSION AS COMMISSION\n" +
                "FROM PERSON AS P\n" +
                "JOIN EMPLOYEE AS E\n" +
                "ON P.PERS_ID = E.PERS_ID\n" +
                "JOIN \n" +
                "(SELECT E.EMPL_ID, SUM(PRIC_PRICE) AS TOTAL\n" +
                "FROM EMPLOYEE AS E\n" +
                "JOIN DEAL AS D\n" +
                "ON E.EMPL_ID = D.EMPL_ID\n" +
                "JOIN PRICE AS PRI\n" +
                "ON D.DEAL_ID = PRI.DEAL_ID\n" +
                "WHERE DEAL_STATUS = 'CLOSED'\n" +
                "GROUP BY E.EMPL_ID\n" +
                "ORDER BY SUM(PRIC_PRICE) DESC) AS EMP_TOTAL\n" +
                "ON E.EMPL_ID = EMP_TOTAL.EMPL_ID\n" +
                "LIMIT 3;",null,null,false,false));
        
    }
       

    public int GetTotalQueries()
    {
        return m_queryArray.size();
    }
    
    public int GetParameterAmtForQuery(int queryChoice)
    {
        QueryData e=m_queryArray.get(queryChoice);
        return e.GetParmAmount();
    }
              
    public String  GetParamText(int queryChoice, int parmnum )
    {
       QueryData e=m_queryArray.get(queryChoice);        
       return e.GetParamText(parmnum); 
    }   

    public String GetQueryText(int queryChoice)
    {
        QueryData e=m_queryArray.get(queryChoice);
        return e.GetQueryString();        
    }
    
    /**
     * Function will return how many rows were updated as a result
     * of the update query
     * @return Returns how many rows were updated
     */
    
    public int GetUpdateAmount()
    {
        return m_updateAmount;
    }
    
    /**
     * Function will return ALL of the Column Headers from the query
     * @return Returns array of column headers
     */
    public String [] GetQueryHeaders()
    {
        return m_jdbcData.GetHeaders();
    }
    
    /**
     * After the query has been run, all of the data has been captured into
     * a multi-dimensional string array which contains all the row's. For each
     * row it also has all the column data. It is in string format
     * @return multi-dimensional array of String data based on the resultset 
     * from the query
     */
    public String[][] GetQueryData()
    {
        return m_jdbcData.GetData();
    }

    public String GetProjectTeamApplication()
    {
        return m_projectTeamApplication;        
    }
    public boolean  isActionQuery (int queryChoice)
    {
        QueryData e=m_queryArray.get(queryChoice);
        return e.IsQueryAction();
    }
    
    public boolean isParameterQuery(int queryChoice)
    {
        QueryData e=m_queryArray.get(queryChoice);
        return e.IsQueryParm();
    }
    
     
    public boolean ExecuteQuery(int queryChoice, String [] parms)
    {
        boolean bOK = true;
        QueryData e=m_queryArray.get(queryChoice);        
        bOK = m_jdbcData.ExecuteQuery(e.GetQueryString(), parms, e.GetAllLikeParams());
        return bOK;
    }
    
     public boolean ExecuteUpdate(int queryChoice, String [] parms)
    {
        boolean bOK = true;
        QueryData e=m_queryArray.get(queryChoice);        
        bOK = m_jdbcData.ExecuteUpdate(e.GetQueryString(), parms);
        m_updateAmount = m_jdbcData.GetUpdateCount();
        return bOK;
    }   
    
      
    public boolean Connect(String szHost, String szUser, String szPass, String szDatabase)
    {

        boolean bConnect = m_jdbcData.ConnectToDatabase(szHost, szUser, szPass, szDatabase);
        if (bConnect == false)
            m_error = m_jdbcData.GetError();        
        return bConnect;
    }
    
    public boolean Disconnect()
    {
        // Disconnect the JDBCData Object
        boolean bConnect = m_jdbcData.CloseDatabase();
        if (bConnect == false)
            m_error = m_jdbcData.GetError();
        return true;
    }
    
    public String GetError()
    {
        return m_error;
    }
 
    private QueryJDBC m_jdbcData;
    private String m_error;    
    private String m_projectTeamApplication;
    private ArrayList<QueryData> m_queryArray;  
    private int m_updateAmount;
            
    /**
     * @param args the command line arguments
     */
    

    
    public static void main(String[] args) {
        // TODO code application logic here

        final QueryRunner queryrunner = new QueryRunner();
        
        if (args.length == 0)
        {
            java.awt.EventQueue.invokeLater(new Runnable() {
                public void run() {

                    new QueryFrame(queryrunner).setVisible(true);
                }            
            });
        }
        else
        {
            if (args[0].equals ("-console"))
            {
            	System.out.println("Nothing has been implemented yet. Please implement the necessary code");
               // TODO 
                // You should code the following functionality:

                //    You need to determine if it is a parameter query. If it is, then
                //    you will need to ask the user to put in the values for the Parameters in your query
                //    you will then call ExecuteQuery or ExecuteUpdate (depending on whether it is an action query or regular query)
                //    if it is a regular query, you should then get the data by calling GetQueryData. You should then display this
                //    output. 
                //    If it is an action query, you will tell how many row's were affected by it.
                // 
                //    This is Psuedo Code for the task:  
                //    Connect()
                //    n = GetTotalQueries()
                //    for (i=0;i < n; i++)
                //    {
                //       Is it a query that Has Parameters
                //       Then
                //           amt = find out how many parameters it has
                //           Create a paramter array of strings for that amount
                //           for (j=0; j< amt; j++)
                //              Get The Paramater Label for Query and print it to console. Ask the user to enter a value
                //              Take the value you got and put it into your parameter array
                //           If it is an Action Query then
                //              call ExecuteUpdate to run the Query
                //              call GetUpdateAmount to find out how many rows were affected, and print that value
                //           else
                //               call ExecuteQuery 
                //               call GetQueryData to get the results back
                //               print out all the results
                //           end if
                //      }
                //    Disconnect()


                // NOTE - IF THERE ARE ANY ERRORS, please print the Error output
                // NOTE - The QueryRunner functions call the various JDBC Functions that are in QueryJDBC. If you would rather code JDBC
                // functions directly, you can choose to do that. It will be harder, but that is your option.
                // NOTE - You can look at the QueryRunner API calls that are in QueryFrame.java for assistance. You should not have to 
                //    alter any code in QueryJDBC, QueryData, or QueryFrame to make this work.
//                System.out.println("Please write the non-gui functionality");
                
            }
        }
 
    }    
}
