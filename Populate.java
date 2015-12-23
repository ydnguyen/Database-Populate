import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.*;

//Purpose: parse JSON files and populate the created DB tables; requires the JSON files to have square brackets [ ]
//at the beginning and end of file since JSON file is being parsed as JSON array
//Usage: java yelp_populate.java yelp_business.json yelp_review.json yelp_checkin.json yelp_user.json

public class Populate
{
    //JDBC Thin connection info; change as needed
    final static String connLink = "jdbc:oracle:thin:@//localhost:1521/ynguyen";
    final static String username = "scott";
    final static String password = "tiger";
	
	private static String[] mainCats = {"Active Life", "Arts & Entertainment", "Automotive",
		"Car Rental", "Cafes", "Beauty & Spas", "Convenience Stores", "Dentists",
		"Doctors", "Drugstores", "Department Stores", "Education", "Event Planning & Services",
		"Flowers & Gifts", "Food", "Health & Medical", "Home Services", "Home & Garden",
		"Hospitals", "Hotels & Travel", "Hardware Stores", "Grocery", "Medical Centers",
		"Nurseries & Gardening", "Nightlife", "Restaurants", "Shopping", "Transportation"};

	//check if a given category is a main category; if it is, return the index of that category
	private static int belongToCat ( String cat )
	{
		int result = Arrays.asList(mainCats).indexOf(cat);
		return result;
	}
	
	public static void main ( String[] args ) 
	{
		try
		{
			
			Class.forName("oracle.jdbc.OracleDriver");
			
		    Connection myConn = DriverManager.getConnection( connLink, username, password );
			
			//----------------------------remove data from all tables---------------------------
			System.out.println("Emptying table reviews");
			Statement truncateReviewStatement = myConn.createStatement();
			String truncateReviewQuery = "delete from reviews";
			truncateReviewStatement.executeUpdate(truncateReviewQuery);
			truncateReviewStatement.close();
			
			System.out.println("Emptying table yelp_users");
			Statement truncateUserStatement = myConn.createStatement();
			String truncateUserQuery = "delete from yelp_users";
			truncateUserStatement.executeUpdate(truncateUserQuery);
			truncateUserStatement.close();
			
			System.out.println("Emptying table business_sub_category");
			Statement truncateCatStatement = myConn.createStatement();
			String truncateCatQuery = "delete from business_sub_category";
			truncateCatStatement.executeUpdate(truncateCatQuery);
			truncateCatStatement.close();
						
			System.out.println("Emptying table days_open");
			Statement truncateDaysStatement = myConn.createStatement();
			String truncateDaysQuery = "delete from days_open";
			truncateDaysStatement.executeUpdate(truncateDaysQuery);
			truncateDaysStatement.close();
			
			System.out.println("Emptying table yelp_businesses");
			Statement truncateBssnStatement = myConn.createStatement();
			String truncateBssnQuery = "delete from yelp_businesses";
			truncateBssnStatement.executeUpdate(truncateBssnQuery);
			truncateBssnStatement.close();
			
			//----------------------------arg #0: yelp_business.json----------------------------
			System.out.println("Populating table yelp_businesses, days_open, and business_sub_category");
			PreparedStatement myBssnStatement = myConn.prepareStatement(
					"Insert into yelp_businesses "
					+ "(business_id, business_name, business_state, business_city, business_zip, business_rating) "
					+ "values (?, ?, ?, ?, ?, ?)");
			//System.out.println("checkpoint #1");
			PreparedStatement bssnSubCatStatement = myConn.prepareStatement(
					"Insert into business_sub_category (business_id, main_category, sub_category, attributes) values (?, ?, ?, ?)");
			//System.out.println("checkpoint #2");
			BufferedReader bssnReader = new BufferedReader( new FileReader(args[0]) );
			//System.out.println("checkpoint #3");
			while ( true )
			{
				String line = bssnReader.readLine();
				if (line == null)
				{
					break;
				}
				JSONTokener tokener = new JSONTokener(line);
				JSONObject business = new JSONObject(tokener);
				//-------------------grab relevant fields from JSON object------------------------------
				String bssnId = (String) business.get("business_id");
				String bssnName = (String) business.get("name");
				String bssnState = (String) business.get("state");
				String bssnCity = (String) business.get("city");
				double bssnRating = (double) business.get("stars");
				//-------------------search for zip code in full address------------------------------
				String bssnAddr = (String) business.get("full_address");
				Pattern p = Pattern.compile("[0-9]{5}");
				Matcher m = p.matcher(bssnAddr);
				String bssnZip = "";
				while (m.find())
				{
					bssnZip = m.group();
				}
				//-------------------fill in appropriate fields for SQLPlus statement------------------------------
				myBssnStatement.setString(1, bssnId);
				myBssnStatement.setString(2, bssnName);
				myBssnStatement.setString(3, bssnState);
				myBssnStatement.setString(4, bssnCity);
				myBssnStatement.setString(5, bssnZip);
				myBssnStatement.setDouble(6, bssnRating);
				//-------------------execute insert statement for each JSON object------------------------------
				myBssnStatement.execute();
				
				//-------------------populate table of days the business is open------------------------------
				int missingDays = 0;
				JSONObject openDays = (JSONObject) business.get("hours");
				PreparedStatement dayStatement = myConn.prepareStatement(
						"Insert into days_open (business_id) values (?)");
				dayStatement.setString(1, bssnId);
				dayStatement.execute();
				dayStatement.close();
				if (openDays.has("Monday"))
				{
					PreparedStatement updateDayStatement = myConn.prepareStatement(
							"Update days_open set monday='y' where business_id = ?");
					updateDayStatement.setString(1, bssnId);
					updateDayStatement.executeUpdate();
					updateDayStatement.close();
				} else { ++missingDays; }
				if (openDays.has("Tuesday"))
				{
					PreparedStatement updateDayStatement = myConn.prepareStatement(
							"Update days_open set tuesday='y' where business_id = ?");
					updateDayStatement.setString(1, bssnId);
					updateDayStatement.executeUpdate();
					updateDayStatement.close();
				} else { ++missingDays; }
				if (openDays.has("Wednesday"))
				{
					PreparedStatement updateDayStatement = myConn.prepareStatement(
							"Update days_open set wednesday='y' where business_id = ?");
					updateDayStatement.setString(1, bssnId);
					updateDayStatement.executeUpdate();
					updateDayStatement.close();
				} else { ++missingDays; }
				if (openDays.has("Thursday"))
				{
					PreparedStatement updateDayStatement = myConn.prepareStatement(
							"Update days_open set thursday='y' where business_id = ?");
					updateDayStatement.setString(1, bssnId);
					updateDayStatement.executeUpdate();
					updateDayStatement.close();
				} else { ++missingDays; }
				if (openDays.has("Friday"))
				{
					PreparedStatement updateDayStatement = myConn.prepareStatement(
							"Update days_open set friday='y' where business_id = ?");
					updateDayStatement.setString(1, bssnId);
					updateDayStatement.executeUpdate();
					updateDayStatement.close();
				} else { ++missingDays; }
				if (openDays.has("Saturday"))
				{
					PreparedStatement updateDayStatement = myConn.prepareStatement(
							"Update days_open set saturday='y' where business_id = ?");
					updateDayStatement.setString(1, bssnId);
					updateDayStatement.executeUpdate();
					updateDayStatement.close();
				} else { ++missingDays; }
				if (openDays.has("Sunday"))
				{
					PreparedStatement updateDayStatement = myConn.prepareStatement(
							"Update days_open set sunday='y' where business_id = ?");
					updateDayStatement.setString(1, bssnId);
					updateDayStatement.executeUpdate();
					updateDayStatement.close();
				} else { ++missingDays; }
				//if all days missing, assume open 7 days a week
				if ( missingDays == 7 ) 
				{
					PreparedStatement updateDayStatement = myConn.prepareStatement(
							"Update days_open set monday = 'y', tuesday = 'y', wednesday = 'y',"
							+ "thursday = 'y', friday = 'y', saturday = 'y', sunday='y' "
							+ "where business_id = ?");
					updateDayStatement.setString(1, bssnId);
					updateDayStatement.executeUpdate();
					updateDayStatement.close();
				}
				
				//------------------------------populate business_sub_category table------------------------------
				JSONArray businessCat = (JSONArray) business.get("categories");
				String bssnAttr = ((JSONObject) business.get("attributes")).toString();
				
				ArrayList<String> mainCats = new ArrayList<String>();
				//grab all the main categories of the business
				for ( Object jObj : businessCat) 
				{
					String cat = (String) jObj;
					int resultIndex = belongToCat(cat);
					if ( resultIndex != -1 ) 
					{
						mainCats.add(cat);
					}
				}
				//for each sub-category, add an entry to the table for each main category
				int subCatCount = 0;
				for ( Object jObj : businessCat)
				{	String subcat = (String) jObj;
					int resultIndex = belongToCat(subcat);
					if ( resultIndex == -1 )
					{
						++subCatCount;
						for ( String s : mainCats) 
						{
							String bssnCatToInsert = "bct" + Integer.toString(belongToCat(s)+1);
							bssnSubCatStatement.setString(1, bssnId);
							bssnSubCatStatement.setString(2, bssnCatToInsert);
							bssnSubCatStatement.setString(3, subcat);
							bssnSubCatStatement.setString(4, bssnAttr);
							bssnSubCatStatement.execute();
						}
					}
				}
				if (subCatCount == 0)
				{
					for ( String s : mainCats) 
					{
						String bssnCatToInsert = "bct" + Integer.toString(belongToCat(s)+1);
						bssnSubCatStatement.setString(1, bssnId);
						bssnSubCatStatement.setString(2, bssnCatToInsert);
						bssnSubCatStatement.setNull(3, Types.VARCHAR);
						bssnSubCatStatement.setString(4, bssnAttr);
						bssnSubCatStatement.execute();
					}
				}
			}
			myBssnStatement.close();
			bssnSubCatStatement.close();
			bssnReader.close();
			//------------------------------arg #3: yelp_user.json------------------------------
			System.out.println("Populating table yelp_users");
			PreparedStatement myUsrStatement = myConn.prepareStatement(
					"Insert into yelp_users (user_id, user_name) values (?, ?)");
			BufferedReader userReader = new BufferedReader(new FileReader (args[3]));
			while (true)
			{
				String line = userReader.readLine();
				if (line == null)
				{
					break;
				}
				JSONTokener tokener = new JSONTokener(line);
				JSONObject user = new JSONObject (tokener);
				//-------------------grab relevant fields from JSON object------------------------------
				String usrName = (String) user.get("name");
				String usrId = (String) user.get("user_id");
				//System.out.println(usrName);
				//-------------------fill in appropriate fields for SQLPlus statement------------------------------
				myUsrStatement.setString(1, usrId);
				myUsrStatement.setString(2, usrName);
				//-------------------execute insert statement for each JSON object------------------------------
				myUsrStatement.execute();
			}
			myUsrStatement.close();
			userReader.close();
			//------------------------------arg #1: yelp_review.json------------------------------
			System.out.println("Populating table reviews");
			PreparedStatement myReviewStatement = myConn.prepareStatement(
					"Insert into reviews "
					+ "(review_id, rating, author, business_id, review_date, review_text) "
					+ "values (?, ?, ?, ?, to_date( ?, 'yyyy-mm-dd' ), ?)");
			BufferedReader reviewReader = new BufferedReader( new FileReader(args[1]));
			while ( true )
			{
				String line = reviewReader.readLine();
				if (line == null)
				{
					break;
				}
				JSONTokener tokener = new JSONTokener(line);
				JSONObject review = new JSONObject(tokener);
				//-------------------grab relevant fields from JSON object------------------------------
				String reviewId = (String) review.get("review_id");
				int rating = review.getInt("stars");
				String author = (String) review.get("user_id");
				String businessId = (String) review.get("business_id");
				String reviewText = (String) review.get("text");
				String reviewDate = (String) review.get("date");
				//-------------------fill in appropriate fields for SQLPlus statement------------------------------
				myReviewStatement.setString(1, reviewId);
				myReviewStatement.setInt(2, rating);
				myReviewStatement.setString(3, author);
				myReviewStatement.setString(4, businessId);
				Blob blob = myConn.createBlob();
				blob.setBytes(1, reviewText.getBytes());
				myReviewStatement.setString(5, reviewDate);
				myReviewStatement.setBlob(6, blob);
				//-------------------execute insert statement for each JSON object------------------------------
				myReviewStatement.execute();
			}
			myReviewStatement.close();
			reviewReader.close();
			System.out.println("Finished populating tables!");
		}
		catch (SQLException sqlEx)
		{
			System.out.println("SQL exception encountered: ");
			//System.out.println(businessError);
			System.out.println(sqlEx.getMessage());
		}
		/*catch (ParseException ex)
		{
			System.out.println("Parse exception found!");
		}*/
		catch (ClassNotFoundException ex) {}
		catch (IOException ex) {}
		catch (Exception ex) { 
			//System.out.println (ex.getMessage());
			System.out.println( "Exception found!" );
		}
	}
}
