import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class DBConnector {
	static final String XML_INPUT_FILE = "arwiki-20151201-pages-articles.xml";
	static final String outputFolder = "real_run_1_to_1";
	static final boolean parsingEnglish = false;
	static final int folder_n = 2;

	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost:3306/wikidb";

	static int rowCount;
	static int folderSize;

	static final String USER = "root";
	static final String PASS = "";

	static Logger log = Logger.getLogger(DBConnector.class);

	public static void main(String[] args) {

		Connection con = null;
		Statement stmt = null;

		try {
			log.info("Connecting To DB...");

			Class.forName("com.mysql.jdbc.Driver");

			// connecting to the database
			con = DriverManager.getConnection(DB_URL, USER, PASS);

			// creating statment object
			stmt = con.createStatement();

			// SQL statment we want to execute
			String sql = "select * from wiki";

			log.info("executing : " + sql);

			ResultSet rs = stmt.executeQuery(sql); // the result of the query

			rowCount = 0;
			if (rs.last()) {
				rowCount = rs.getRow();
				rs.beforeFirst(); // not rs.first() because the rs.next() below
									// will move on, missing the first element
			}

			log.info("Result set size: " + rowCount);

			folderSize = (int) Math.ceil((double)rowCount / (double)folder_n);

			HashMap<Integer, Integer> IDs = new HashMap<Integer, Integer>();
			HashMap<Integer, Integer> folderMap = new HashMap<Integer, Integer>();
			HashMap<Integer, Boolean> IdState = new HashMap<Integer, Boolean>();
			HashMap<Integer, Integer> repeated = new HashMap<>();

			int c2 = 0;
			
			while (rs.next()) {
				int incID = rs.getInt("id");
				int enID = rs.getInt("en_id");
				int arID = rs.getInt("ar_id");
				
				int incIDMap = getFolderMap(incID);
				
				// if we parsing english documents we put the english id as key
				// and arabic as value and vice verca
				if (parsingEnglish) {
					IdState.put(enID, false);

					if (repeated.containsKey(enID)) {
						repeated.put(enID, repeated.get(enID) + 1);
						c2++;
					} else {
						repeated.put(enID, 1);
						IDs.put(enID, arID);
						folderMap.put(enID, incIDMap);
					}
				} else {
					IdState.put(arID, false);

					if (repeated.containsKey(arID)) {
						repeated.put(arID, repeated.get(arID) + 1);
						c2++;
					} else {
						repeated.put(arID, 1);
						IDs.put(arID, enID);
						folderMap.put(arID, incIDMap);
					}
				}
			}

			rs.close();
			stmt.close();
			con.close();

			int x = 0;
			for (Map.Entry<Integer, Integer> ii : repeated.entrySet()) {
				if (ii.getValue() > 1) {
					System.out.println(ii.getKey() + " : " + ii.getValue());
					x++;
				}
			}

			System.out.println("****" + c2 + "****");
			System.out.println(x);
			log.info("DB loaded into The HashMap");
			log.info(IDs.size() + " Records added.");

			// ParserHandler.Parse(XML_INPUT_FILE,outputFolder, IDs,
			// parsingEnglish);
			// DOMReader.Parser(XML_INPUT_FILE,outputFolder, IDs,
			// parsingEnglish);

			DOMReaderHandlerV2.Parser(XML_INPUT_FILE, outputFolder, IDs, parsingEnglish, IdState, folderMap);

			log.info("Processing Terminated.");
		} catch (Exception ex) {
			log.info(ex);
		}

	}

	static int getFolderMap(int id_increment) {
		return (int) Math.ceil((double)id_increment / (double)folderSize);
	}

}
