/*
 * Name: Steven Lim
 * PID: A10565388
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PA2
{
    public static void main(String[] args) throws ClassNotFoundException
    {
        // load the sqlite-JDBC driver using the current class loader
        Class.forName("org.sqlite.JDBC");

        Connection connection = null;
        try
        {
            // create a database connection
            connection = DriverManager.getConnection("jdbc:sqlite:pa2.db");
            Statement statement = connection.createStatement();

            // Counter to count how many stops
            int counter = 1;

            // check to see if Connected table has been completed
            int delta = 1;

            // Create Connected table with 0 values for all entries
            statement.executeUpdate("drop table if exists Connected;");
            statement.executeUpdate("create table Connected (Airline string, Origin string, Destination char(32), "
                                   + "Stops integer);");
            statement.executeUpdate("insert into Connected select Airline, Origin, Destination, 0 from Flight;");
            
            // Table to keep track of the up to date schedule
            statement.executeUpdate("drop table if exists upToDate;");
            statement.executeUpdate("create table upToDate as select * from Flight;");

            // Table to keep track of the changes occuring
            statement.executeUpdate("drop table if exists chFlights;");
            statement.executeUpdate("create table chFlights as select * from Flight;");
       
            while(true)
            {
                // Create table to hold value from the updated table temporarily
                statement.executeUpdate("drop table if exists temp;");
                statement.executeUpdate("create table temp as select * from upToDate;");

                // Create table to get the most current schedule being looked at
                statement.executeUpdate("drop table if exists upToDate;");
                statement.executeUpdate("create table upToDate as select * from temp union "
                                        + "select ft.Airline, ft.Origin, ch.Destination from Flight ft, chFlights ch " 
                                        + "where ft.Destination = ch.Origin and ft.Origin <> ch.Destination "
                                        + "and ft.Destination <> ch.Destination and ft.Origin <> ch.Origin and ft.Airline = ch.Airline;");

                // Create table to get rid of out of date information from previous iterations
                statement.executeUpdate("drop table if exists chFlights;");
                statement.executeUpdate("create table chFlights as select * from upToDate except select * from temp;");

                // Put updated values into Connected table
                // returns 0 when there are no more rows to be inserted to. Connected table is done
                // when that happens.
                delta = statement.executeUpdate("insert into Connected select Airline, Origin, Destination, "
                                        + counter + " from chFlights;");

                // If delta is 0, then Connected table has been completed and we should break 
                // out of loop
                if(delta == 0){
                    break;
                }

                // Increment the Stops counter 
                counter++;
            }

            // For Result 1 Output
            //ResultSet rset = statement.executeQuery("select * from Connected ORDER BY Origin, Destination");
            
            // For Result 2 Output
            ResultSet rset = statement.executeQuery("select * from Connected ORDER BY Airline, Origin;");
            
            /*while (rset.next()) {
                // Get the attribute value.
                System.out.print(rset.getString("Airline"));
                System.out.print("---");
                System.out.print(rset.getString("Origin"));
                System.out.print("---");
                System.out.print(rset.getString("Destination"));
                System.out.print("---");
                System.out.println(rset.getString("Stops"));
            }*/

            statement.close();
            rset.close();
        }
        catch(SQLException e)
        {
            // if the error message is "out of memory", 
            // it probably means no database file is found
            System.err.println(e.getMessage());
        }
        finally
        {
            try
            {
                if(connection != null)
                    connection.close();
            }
            catch(SQLException e)
            {
                // connection close failed.
                System.err.println(e);
            }
        }
    }
}
