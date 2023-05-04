package Miliestone1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;
import exceptions.DBAppException;

public class DBApp implements Serializable {

	public static int maxRows;
	public static int maxEntries;

	public DBApp() {
		try {
			init();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void readConfigFile() {
		Properties props = new Properties();
		try {
			// Load the configuration file
			FileInputStream in = new FileInputStream("src/resources/DBApp.config");
			props.load(in);
			in.close();

			// Get the values of the configuration parameters
			maxRows = Integer.parseInt(props.getProperty("MaximumRowsCountinTablePage"));
			maxEntries = Integer.parseInt(props.getProperty("MaximumEntriesinOctreeNode"));

			// Do something with the configuration parameters , this was just for testing
			// System.out.println("Maximum rows per page: " + maxRows);
			// System.out.println("Maximum entries per octree node: " + maxEntries);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void init() throws IOException {
		// this does whatever initialization you would like
		readConfigFile();

		File resourceDirectory = new File("src/resources");
		if (!resourceDirectory.exists())
			resourceDirectory.mkdirs();

		File tableDirectory = new File("src/resources/tables");
		if (!tableDirectory.exists())
			tableDirectory.mkdirs();

		File dbAppConfig = new File("src/resources/DBApp.config");
		if (!dbAppConfig.exists())
			dbAppConfig.createNewFile();

	}

	public static boolean checkInput(String TableName, Hashtable<String, Object> input)
			throws IOException, DBAppException {
		// for(Object key: input.keySet()) {
		// System.out.println(key.hashCode() + "\t" + key + "\t" + input.get(key) );
		// this was for testing purpose
		// }
		// System.out.println("hey");
		for (String key : input.keySet()) {
			if (!checkDataType(TableName, key, input.get(key))) {
				// System.out.println("hey2");
				throw new DBAppException();
			}
			// System.out.println("hey7");
		}
		return true;

	}

	public static boolean checkDataType(String tableName, String coulmn, Object input)
			throws IOException, DBAppException {
		BufferedReader br = new BufferedReader(new FileReader("metadata.csv"));
		String line = "";
		while ((line = br.readLine()) != null) {
			String[] content = line.split(",");
			// for(int i = 0;i<content.length;i++) {
			// System.out.print(content[i] + " ");
			// }

			// System.out.println();
			// line = br.readLine();
			if (content[0].equals(tableName)) {
				if (coulmn.equals(content[1])) {
					switch (content[2]) {
						case "java.lang.Double":
							if (input instanceof Double) {
								// System.out.println("hey3");
								return true;
							} else
								return false;
						case "java.lang.String":
							if (input instanceof String) {
								// System.out.println("hey4");
								return true;
							} else
								return false;
						case "java.lang.Integer":
							if (input instanceof Integer) {
								// System.out.println("hey5");
								return true;
							} else
								return false;
						case "java.util.Date":
							if (input instanceof Date) {
								// System.out.println("hey6");
								return true;
							} else
								return false;
					}
				}
			} else
				line = br.readLine();
		}
		br.close();
		return false;

	}

	public static boolean newTableValidation(String strTableName,
			String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax) throws IOException {

		// BufferedReader br = new BufferedReader(new FileReader("metadata.csv"));
		// String line = "";
		// while((line = br.readLine())!= null) {
		// System.out.println("fuck off");
		// String [] content = line.split(",");
		// if(content[0].equals(strTableName)) {
		// System.out.println("false 1");
		// return false;
		// }
		//
		// line = br.readLine();
		// }
		// br.close();

		if (!htblColNameType.containsKey(strClusteringKeyColumn)) {
			// System.out.println("false2");
			return false;
		}
		for (String key : htblColNameType.keySet()) {
			if (!(htblColNameType.get(key).equals("java.lang.Integer")
					|| htblColNameType.get(key).equals("java.lang.String")
					|| htblColNameType.get(key).equals("java.lang.Double")
					|| htblColNameType.get(key).equals("java.util.Date"))) {
				// System.out.println("false3");
				return false;
			}
		}

		for (String key : htblColNameMin.keySet()) {
			// System.out.println("min");
			switch (key) {
				case "java.lang.Integer":
					try {
						Integer.parseInt(htblColNameMin.get(key));
						break;
					} catch (NumberFormatException e) {
						// System.out.println("false4");
						return false;
					}
				case "java.lang.Double":
					try {
						Double.parseDouble(htblColNameMin.get(key));
						break;
					} catch (NumberFormatException e) {
						// System.out.println("false5");
						return false;
					}
				case "java.util.Date":
					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
					dateFormat.setLenient(false);
					try {
						dateFormat.parse(htblColNameMin.get(key).trim());
						// return true;
						break;
					} catch (ParseException e) {
						// System.out.println("false6");
						return false;
					}
				case "java.lang.String":
					if (htblColNameMin.get(key).equals("null")) {
						// System.out.println("false7");
						return false;
					}
					break;

				default: // System.out.println("false8");
					return false;
			}
		}

		for (String key : htblColNameMax.keySet()) {
			switch (key) {
				case "java.lang.Integer":
					try {
						Integer.parseInt(htblColNameMax.get(key));
						break;
					} catch (NumberFormatException e) {
						// System.out.println("false9");
						return false;
					}
				case "java.lang.Double":
					try {
						Double.parseDouble(htblColNameMax.get(key));
						break;
					} catch (NumberFormatException e) {
						// System.out.println("false10");
						return false;
					}
				case "java.util.Date":
					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
					dateFormat.setLenient(false);
					try {
						dateFormat.parse(htblColNameMax.get(key).trim());
						// return true;
						break;
					} catch (ParseException e) {
						// System.out.println("false11");
						return false;
					}
				case "java.lang.String":
					if (htblColNameMax.get(key).equals("null")) {
						// System.out.println("false12");
						return false;
					}

					break;

				default: // System.out.println("false13");
					return false;
			}
		}

		// System.out.println("true");
		return true;

	}

	public void serialize(String path) throws IOException {
		FileOutputStream fileOut2 = new FileOutputStream(path);
		ObjectOutputStream out2 = new ObjectOutputStream(fileOut2);
		out2.writeObject(this);
		out2.close();
		fileOut2.close();
	}

	public Table deserializeTable(String path) throws IOException, ClassNotFoundException {
		Table table = null;
		FileInputStream fileIn = new FileInputStream(path);
		ObjectInputStream in = new ObjectInputStream(fileIn);
		table = (Table) in.readObject();
		in.close();
		fileIn.close();
		return table;
	}

	public Page deserializePage(String path) throws IOException, ClassNotFoundException {
		Page page = null;
		FileInputStream fileIn = new FileInputStream(path);
		ObjectInputStream in = new ObjectInputStream(fileIn);
		page = (Page) in.readObject();
		in.close();
		fileIn.close();
		return page;
	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax) throws DBAppException, IOException {

		if (newTableValidation(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax)) {

			// File folder = new File("src/resources/"+strTableName);
			//
			// if(!folder.exists()) {
			// boolean created = folder.mkdir();
			// if(!created) {
			// throw new DBAppException();
			// }
			// }

			Table table = new Table(strTableName, strClusteringKeyColumn);

			File folder = new File("src/resources/tables/" + strTableName);

			if (folder.exists())
				throw new DBAppException();
			else
				folder.mkdir();

			folder = new File("src/resources/tables/" + strTableName + "/pages");

			if (folder.exists())
				throw new DBAppException();
			else
				folder.mkdir();

			FileOutputStream fileOut2 = new FileOutputStream(
					"src/resources/tables/" + strTableName + "/" + strTableName + ".ser");
			ObjectOutputStream out2 = new ObjectOutputStream(fileOut2);
			out2.writeObject(table);
			out2.close();
			fileOut2.close();

			String MetaData = "metadata.csv";

			BufferedWriter writer = new BufferedWriter(new FileWriter(MetaData, true));
			// The second argument "true" in FileWriter constructor
			// indicates that data should be appended to the file rather than overwritten.

			StringBuilder sb = new StringBuilder();
			for (String key : htblColNameType.keySet()) {
				sb.append(strTableName + ",");
				sb.append(key + "," + htblColNameType.get(key) + ",");
				if (key.equals(strClusteringKeyColumn))
					sb.append("True,");
				else
					sb.append("False,");
				sb.append("null,null,");

				for (String min : htblColNameMin.keySet()) {
					if (htblColNameType.get(key).equals(min)) {
						sb.append(htblColNameMin.get(min) + ",");
						break;
					}
				}

				for (String max : htblColNameMax.keySet()) {
					// System.out.println(max);
					// System.out.println(htblColNameType.keySet());
					if (htblColNameType.get(key).equals(max)) {
						sb.append(htblColNameMax.get(max) + "\n");
						break;
					}
				}
			}
			// sb.append("\r\n");
			writer.write(sb.toString());
			writer.close();
			// System.out.println("Csv file is altered successfully");
		}
	}

	public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {

	}

	public Object getPrimaryKey(Table table, Hashtable<String, Object> input) {
		return input.get(table.primaryKey);
	}

	public static Hashtable<String, Object> getMinMax(String str, String tableName) throws IOException, DBAppException {
		Hashtable<String, Object> result = new Hashtable<String, Object>();
		int csvColumn = 0;
		str = str.toLowerCase();
		if (str.equals("min"))
			csvColumn = 6;
		else
			csvColumn = 7;
		BufferedReader br = new BufferedReader(new FileReader("metadata.csv"));
		String line = "";
		while ((line = br.readLine()) != null) {
			// System.out.println("angela");
			String[] content = line.split(",");
			// for(int i = 0;i<content.length;i++) {
			// System.out.print(content[i] + " ");
			// }
			// System.out.println();
			if (content.length != 0 && content[0].equals(tableName)) {
				String key = content[1];
				// System.out.println(key);
				switch (content[2]) {
					case "java.lang.Integer":
						result.put(key, Integer.parseInt(content[csvColumn]));
						// System.out.println("Castint");
						break;
					case "java.lang.Double":
						result.put(key, Double.parseDouble(content[csvColumn]));
						// System.out.println("CastDouble");
						break;
					case "java.util.Date":
						SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

						try {
							Date date = formatter.parse(content[csvColumn]);
							result.put(key, date);
							break;
						} catch (ParseException e) {
							throw new DBAppException();
						}
					case "java.lang.String": // System.out.println("Caststring");
						result.put(key, content[csvColumn]);
						break;

					default:
						throw new DBAppException();
				}

			}
		}
		br.close();
		return result;
	}

	public static boolean checkMinMax(Hashtable<String, Object> input, String tableName)
			throws IOException, DBAppException {
		// System.out.println(input);
		Hashtable<String, Object> min = getMinMax("min", tableName);
		Hashtable<String, Object> max = getMinMax("max", tableName);

		for (String key : input.keySet()) {
			Object minKey = min.get(key);
			Object maxKey = max.get(key);

			if (input.get(key) instanceof Integer) {
				int inputKey = (int) input.get(key);

				if (inputKey < (int) minKey || inputKey > (int) maxKey) {
					// System.out.println("int fail");
					return false;
				}
			} else if (input.get(key) instanceof Double) {
				double inputKey = (double) input.get(key);

				if (inputKey < (double) minKey || inputKey > (double) maxKey) {
					// System.out.println("double fail");
					return false;
				}
			}

			else if (input.get(key) instanceof String) {
				String inputKey = (String) input.get(key);
				// inputKey.toUpperCase();
				// System.out.println(inputKey);
				// System.out.println(inputKey.compareTo((String)minKey));
				// System.out.println(inputKey.compareTo((String)maxKey));
				if (!(inputKey.compareTo((String) minKey) >= 0 && inputKey.compareTo((String) maxKey) <= 0)) {
					// System.out.println("String fail");
					return false;
				}
			} else if (input.get(key) instanceof Date) {
				Date inputKey = (Date) input.get(key);

				if (inputKey.compareTo((Date) minKey) < 0 || inputKey.compareTo((Date) maxKey) > 0)
					return false;
			}
		}
		return true;
	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) // htblColNameValue -->
																									// input
			throws DBAppException, IOException, ClassNotFoundException {

		Table table = null;
		FileInputStream fileIn = new FileInputStream(
				"src/resources/tables/" + strTableName + "/" + strTableName + ".ser");
		ObjectInputStream in = new ObjectInputStream(fileIn);
		table = (Table) in.readObject();
		in.close();
		fileIn.close();

		Object Pk = getPrimaryKey(table, htblColNameValue); // ---> Primary key
		if (Pk == null)
			throw new DBAppException();
		if (!checkIfDuplicate(table, Pk)) {
			throw new DBAppException();
		}

		if (!checkMinMax(htblColNameValue, table.Name))
			throw new DBAppException();
		// inserting in an empty table, creating new page and inserting in it
		if (table.serializedPages.size() == 0) {
			Page firstPage = new Page();
			if (checkInput(strTableName, htblColNameValue)) {
				firstPage.pageVector.add(htblColNameValue);
				// firstPage.minValue =Pk;
				// firstPage.maxValue =Pk;
				Pair p = new Pair(Pk, Pk);
				table.MinMax.add(p);
				// System.out.println(firstPage.pageVector.size());
			}
			// serializing the page to add its file path to the vector in the table class
			FileOutputStream fileOut = new FileOutputStream(
					"src/resources/tables/" + strTableName + "/pages/Page " + table.pageCounter + ".ser");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(firstPage);
			out.close();
			fileOut.close();
			table.serializedPages
					.add("src/resources/tables/" + strTableName + "/pages/Page " + table.pageCounter + ".ser");
			table.pageCounter++;
			// System.out.println("insert 1");

		} else {
			if (table.serializedPages.size() == 1) {
				Page currentPage = null;
				FileInputStream fileIn2 = new FileInputStream(table.serializedPages.get(0));
				ObjectInputStream in2 = new ObjectInputStream(fileIn2);
				currentPage = (Page) in2.readObject();
				in2.close();
				fileIn2.close();
				Object newRowPk = getPrimaryKey(table, htblColNameValue);
				if (currentPage.pageVector.size() < maxRows) {
					if (checkInput(table.Name, htblColNameValue)) {
						int index = getIndexInsert(currentPage.pageVector, newRowPk, table);
						currentPage.pageVector.insertElementAt(htblColNameValue, index);
					}
					Pair p = table.MinMax.get(0);
					// updatePair(p,newRowPk);
					// serializing back the page
					FileOutputStream fileOut = new FileOutputStream(table.serializedPages.get(0));
					ObjectOutputStream out = new ObjectOutputStream(fileOut);
					out.writeObject(currentPage);
					out.close();
					fileOut.close();
					// System.out.println("insert 2");
					// System.out.println(currentPage.pageVector.size());

				} else {
					Page newPage = new Page();
					Pair p = table.MinMax.get(0);
					if (modifiedCompareTo(newRowPk, p.getMax()) > 0) {
						if (checkInput(table.Name, htblColNameValue)) {
							newPage.pageVector.add(htblColNameValue);
						}

					} else if (modifiedCompareTo(newRowPk, p.getMax()) < 0) {
						int index = getIndexInsert(currentPage.pageVector, newRowPk, table);
						// System.out.println(index);
						if (checkInput(table.Name, htblColNameValue)) {
							currentPage.pageVector.insertElementAt(htblColNameValue, index);
							// for(int i = 0 ;i<currentPage.pageVector.size();i++) {
							// System.out.println(currentPage.pageVector.get(i));
							// }
						}

						// System.out.println(currentPage.pageVector.get(4));
						newPage.pageVector.add(currentPage.pageVector.get(maxRows));
						currentPage.pageVector.remove(maxRows);
						// table.MinMax.get(0).setMax(((Hashtable)(currentPage.pageVector.get(maxRows-1))).get(table.primaryKey));
					}

					// updatePair(p,newRowPk);
					// ((Hashtable)(newPage.pageVector.get(0))).get(table.primaryKey);
					Pair page2Pair = new Pair(((Hashtable) (newPage.pageVector.get(0))).get(table.primaryKey),
							((Hashtable) (newPage.pageVector.get(0))).get(table.primaryKey));
					table.MinMax.add(page2Pair);
					FileOutputStream fileOut = new FileOutputStream(
							"src/resources/tables/" + strTableName + "/pages/Page " + table.pageCounter + ".ser");
					ObjectOutputStream out = new ObjectOutputStream(fileOut);
					out.writeObject(newPage);
					out.close();
					fileOut.close();
					table.serializedPages
							.add("src/resources/tables/" + strTableName + "/pages/Page " + table.pageCounter + ".ser");
					table.pageCounter++;

					FileOutputStream fileOut1 = new FileOutputStream(table.serializedPages.get(0));
					ObjectOutputStream out1 = new ObjectOutputStream(fileOut1);
					out1.writeObject(currentPage);
					out1.close();
					fileOut1.close();

					updatePair(table.MinMax, table.serializedPages, table.primaryKey);
					// System.out.println("insert 3");
					// System.out.println(currentPage.pageVector.size());
				}
			} else {
				int pageIndex = getPageIndex(table.MinMax, Pk);
				// System.out.println("pageIndex: " + pageIndex);
				if (pageIndex == -1) {
					String pagePath = table.serializedPages.get(table.serializedPages.size() - 1);
					//
					Page currentPage = null;
					FileInputStream fileIn3 = new FileInputStream(pagePath);
					ObjectInputStream in3 = new ObjectInputStream(fileIn3);
					currentPage = (Page) in3.readObject();
					in3.close();
					fileIn3.close();
					//
					if (currentPage.pageVector.size() != maxRows) {
						currentPage.pageVector.add(htblColNameValue);
						FileOutputStream fileOut = new FileOutputStream(pagePath);
						ObjectOutputStream out = new ObjectOutputStream(fileOut);
						out.writeObject(currentPage);
						out.close();
						fileOut.close();
					} else {
						Page newPage = new Page();
						newPage.pageVector.add(htblColNameValue);
						Pair p = new Pair(1, 1);
						table.MinMax.add(p);
						FileOutputStream fileOut = new FileOutputStream(
								"src/resources/tables/" + strTableName + "/pages/Page " + table.pageCounter + ".ser");
						ObjectOutputStream out = new ObjectOutputStream(fileOut);
						out.writeObject(newPage);
						out.close();
						fileOut.close();
						table.serializedPages.add(
								"src/resources/tables/" + strTableName + "/pages/Page " + table.pageCounter + ".ser");
						table.pageCounter++;
					}

				} else {

					//
					String pagePath = table.serializedPages.get(pageIndex);
					Page currentPage = null;
					FileInputStream fileIn3 = new FileInputStream(pagePath);
					ObjectInputStream in3 = new ObjectInputStream(fileIn3);
					currentPage = (Page) in3.readObject();
					in3.close();
					fileIn3.close();
					//
					int index = getIndexInsert(currentPage.pageVector, Pk, table);
					currentPage.pageVector.insertElementAt(htblColNameValue, index);
					// updatePair(table.MinMax.get(pageIndex),Pk);
					//
					FileOutputStream fileOut = new FileOutputStream(pagePath);
					ObjectOutputStream out = new ObjectOutputStream(fileOut);
					out.writeObject(currentPage);
					out.close();
					fileOut.close();
					//

				}

			}

			// System.out.println("insertion done");
		}
		shiftPages(table);
		updatePair(table.MinMax, table.serializedPages, table.primaryKey);
		// serializing back the table after insertion
		FileOutputStream fileOut = new FileOutputStream(
				"src/resources/tables/" + strTableName + "/" + strTableName + ".ser");
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(table);
		out.close();
		fileOut.close();
	}

	public void shiftPages(Table table) throws IOException, ClassNotFoundException {
		Vector<String> paths = table.serializedPages;
		for (int i = 0; i < paths.size(); i++) {
			Page currentPage = null;
			FileInputStream fileIn = new FileInputStream(paths.get(i));
			ObjectInputStream in = new ObjectInputStream(fileIn);
			currentPage = (Page) in.readObject();
			in.close();
			fileIn.close();
			int j = i + 1;
			if (currentPage.pageVector.size() > maxRows && j != paths.size()) {
				Page followingPage = null;
				FileInputStream fileIn2 = new FileInputStream(paths.get(i + 1));
				ObjectInputStream in2 = new ObjectInputStream(fileIn2);
				followingPage = (Page) in2.readObject();
				in2.close();
				fileIn2.close();
				followingPage.pageVector.insertElementAt(currentPage.pageVector.get(currentPage.pageVector.size() - 1),
						0);
				currentPage.pageVector.remove(currentPage.pageVector.size() - 1);
				FileOutputStream fileOut = new FileOutputStream(paths.get(i + 1));
				ObjectOutputStream out = new ObjectOutputStream(fileOut);
				out.writeObject(followingPage);
				out.close();
				fileOut.close();
			} else if (currentPage.pageVector.size() > maxRows && j == paths.size()) {
				Page newPage = new Page();
				newPage.pageVector.insertElementAt(currentPage.pageVector.get(currentPage.pageVector.size() - 1), 0);
				currentPage.pageVector.remove(currentPage.pageVector.size() - 1);
				Pair pair = new Pair(1, 1);
				table.MinMax.add(pair);
				table.serializedPages
						.add("src/resources/tables/" + table.Name + "/pages/Page " + table.pageCounter + ".ser");
				FileOutputStream fileOut = new FileOutputStream(
						"src/resources/tables/" + table.Name + "/pages/Page " + table.pageCounter + ".ser");
				ObjectOutputStream out = new ObjectOutputStream(fileOut);
				out.writeObject(newPage);
				out.close();
				fileOut.close();
				table.pageCounter++;
			}
			FileOutputStream fileOut = new FileOutputStream(paths.get(i));
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(currentPage);
			out.close();
			fileOut.close();
		}
	}

	public int getPageIndex(Vector<Pair> p, Object pk) {
		if (pk instanceof Integer) {

			// System.out.println("i am an integer");
			int intpk = (int) pk;
			for (int i = 0; i < p.size(); i++) {
				Pair currentPair = p.get(i);
				int min = (int) currentPair.getMin();
				int max = (int) currentPair.getMax();
				if ((intpk > min && intpk < max) || intpk < min)
					return i;
			}
		} else if (pk instanceof Double) {
			// System.out.println("i am a double");
			double doublepk = (double) pk;
			for (int i = 0; i < p.size(); i++) {
				Pair currentPair = p.get(i);
				double min = (double) currentPair.getMin();
				double max = (double) currentPair.getMax();
				if ((doublepk > min && doublepk < max) || doublepk < min)
					return i;
			}
		}

		else if (pk instanceof String) {
			// System.out.println("i am a string");
			String stringpk = (String) pk;
			for (int i = 0; i < p.size(); i++) {
				Pair currentPair = p.get(i);
				String min = (String) currentPair.getMin();
				String max = (String) currentPair.getMax();
				if ((stringpk.compareTo(min) > 0 && stringpk.compareTo(max) < 0) || stringpk.compareTo(min) < 0)
					return i;

			}
		}

		else if (pk instanceof Date) {
			// System.out.println("i am a date");
			Date datepk = (Date) pk;
			for (int i = 0; i < p.size(); i++) {
				Pair currentPair = p.get(i);
				Date min = (Date) currentPair.getMin();
				Date max = (Date) currentPair.getMax();
				if ((datepk.compareTo(min) > 0 && datepk.compareTo(max) < 0) || datepk.compareTo(min) < 0)
					return i;

			}
		}

		return -1;
	}

	public int getPageIndexUpdate(Vector<Pair> p, Object pk) {
		if (pk instanceof Integer) {
			// System.out.println(p);

			// System.out.println("i am an integer");

			int intpk = (int) pk;
			for (int i = 0; i < p.size(); i++) {
				Pair currentPair = p.get(i);
				// System.out.println(currentPair);
				int min = (int) currentPair.getMin();
				int max = (int) currentPair.getMax();
				if ((intpk > min && intpk < max) || intpk == min || intpk == max)
					return i;
			}
		} else if (pk instanceof Double) {
			// System.out.println("i am a double");
			double doublepk = (double) pk;
			for (int i = 0; i < p.size(); i++) {
				Pair currentPair = p.get(i);
				double min = (double) currentPair.getMin();
				double max = (double) currentPair.getMax();
				if ((doublepk > min && doublepk < max) || doublepk < min)
					return i;
			}
		}

		else if (pk instanceof String) {
			// System.out.println("i am a string");
			String stringpk = (String) pk;
			for (int i = 0; i < p.size(); i++) {
				Pair currentPair = p.get(i);
				String min = (String) currentPair.getMin();
				String max = (String) currentPair.getMax();
				if ((stringpk.compareTo(min) > 0 && stringpk.compareTo(max) < 0) || stringpk.compareTo(min) < 0)
					return i;

			}
		}

		else if (pk instanceof Date) {
			// System.out.println("i am a date");
			Date datepk = (Date) pk;
			for (int i = 0; i < p.size(); i++) {
				Pair currentPair = p.get(i);
				Date min = (Date) currentPair.getMin();
				Date max = (Date) currentPair.getMax();
				if ((datepk.compareTo(min) > 0 && datepk.compareTo(max) < 0) || datepk.compareTo(min) < 0)
					return i;

			}
		}

		return -1;
	}

	public static boolean checkIfDuplicate(Table table, Object pk) throws IOException, ClassNotFoundException { // modify
																												// with
																												// binary
																												// search
		Vector<Pair> p = table.MinMax;
		if (pk instanceof Integer) {
			int Intpk = (int) pk;
			for (int i = 0; i < p.size(); i++) {
				Pair currentPair = p.get(i);
				int min = (int) currentPair.getMin();
				int max = (int) currentPair.getMax();
				if (Intpk >= min && Intpk <= max) {
					if (Intpk == min || Intpk == max)
						return false;
					String pagePath = table.serializedPages.get(i);
					Page currentPage = null;
					FileInputStream fileIn = new FileInputStream(pagePath);
					ObjectInputStream in = new ObjectInputStream(fileIn);
					currentPage = (Page) in.readObject();
					in.close();
					fileIn.close();
					Vector pageRecords = currentPage.pageVector;
					int index = getIndexUpdate(pageRecords, pk, table); // uses binary search
					if (index != -1)
						return false;
					// for (int j = 0; j < pageRecords.size(); j++) {
					// Hashtable row = (Hashtable) pageRecords.get(j);
					// if ((int) (row.get(table.primaryKey)) == Intpk)
					// return false;
					// }
					FileOutputStream fileOut = new FileOutputStream(pagePath);
					ObjectOutputStream out = new ObjectOutputStream(fileOut);
					out.writeObject(currentPage);
					out.close();
					fileOut.close();
				}
			}

		} else if (pk instanceof Double) {
			double doublepk = (double) pk;
			for (int i = 0; i < p.size(); i++) {
				Pair currentPair = p.get(i);
				double min = (double) currentPair.getMin();
				double max = (double) currentPair.getMax();
				if (doublepk >= min && doublepk <= max) {
					if (doublepk == min || doublepk == max)
						return false;
					String pagePath = table.serializedPages.get(i);
					Page currentPage = null;
					FileInputStream fileIn = new FileInputStream(pagePath);
					ObjectInputStream in = new ObjectInputStream(fileIn);
					currentPage = (Page) in.readObject();
					in.close();
					fileIn.close();
					Vector pageRecords = currentPage.pageVector;
					int index = getIndexUpdate(pageRecords, pk, table); // uses binary search
					if (index != -1)
						return false;
					// for (int j = 0; j < pageRecords.size(); j++) {
					// Hashtable row = (Hashtable) pageRecords.get(j);
					// if ((double) (row.get(table.primaryKey)) == doublepk)
					// return false;
					// }
					FileOutputStream fileOut = new FileOutputStream(pagePath);
					ObjectOutputStream out = new ObjectOutputStream(fileOut);
					out.writeObject(currentPage);
					out.close();
					fileOut.close();
				}
			}
		} else if (pk instanceof String) {
			String stringPk = (String) pk;
			for (int i = 0; i < p.size(); i++) {
				Pair currentPair = p.get(i);
				String min = (String) currentPair.getMin();
				String max = (String) currentPair.getMax();
				if (stringPk.compareTo(min) >= 0 && stringPk.compareTo(max) <= 0) {
					if (stringPk.compareTo(min) == 0 || stringPk.compareTo(max) == 0)
						return false;
					String pagePath = table.serializedPages.get(i);
					Page currentPage = null;
					FileInputStream fileIn = new FileInputStream(pagePath);
					ObjectInputStream in = new ObjectInputStream(fileIn);
					currentPage = (Page) in.readObject();
					in.close();
					fileIn.close();
					Vector pageRecords = currentPage.pageVector;
					int index = getIndexUpdate(pageRecords, pk, table); // uses binary search
					if (index != -1)
						return false;
					// for (int j = 0; j < pageRecords.size(); j++) {
					// Hashtable row = (Hashtable) pageRecords.get(j);
					// if (((String) (row.get(table.primaryKey))).equals(stringPk))
					// return false;
					// }
					FileOutputStream fileOut = new FileOutputStream(pagePath);
					ObjectOutputStream out = new ObjectOutputStream(fileOut);
					out.writeObject(currentPage);
					out.close();
					fileOut.close();
				}
			}
		} else if (pk instanceof Date) {
			Date datePk = (Date) pk;
			for (int i = 0; i < p.size(); i++) {
				Pair currentPair = p.get(i);
				Date min = (Date) currentPair.getMin();
				Date max = (Date) currentPair.getMax();
				if (datePk.compareTo(min) >= 0 && datePk.compareTo(max) <= 0) {
					if (datePk.compareTo(min) == 0 || datePk.compareTo(max) == 0)
						return false;
					String pagePath = table.serializedPages.get(i);
					Page currentPage = null;
					FileInputStream fileIn = new FileInputStream(pagePath);
					ObjectInputStream in = new ObjectInputStream(fileIn);
					currentPage = (Page) in.readObject();
					in.close();
					fileIn.close();
					Vector pageRecords = currentPage.pageVector;
					int index = getIndexUpdate(pageRecords, pk, table); // uses binary search
					if (index != -1)
						return false;
					// for (int j = 0; j < pageRecords.size(); j++) {
					// Hashtable row = (Hashtable) pageRecords.get(j);
					// if (((Date) (row.get(table.primaryKey))).equals(datePk))
					// return false;
					// }
					FileOutputStream fileOut = new FileOutputStream(pagePath);
					ObjectOutputStream out = new ObjectOutputStream(fileOut);
					out.writeObject(currentPage);
					out.close();
					fileOut.close();
				}
			}
		}

		return true;
	}

	public static void updatePair(Vector<Pair> pairs, Vector<String> paths, String pk)
			throws IOException, ClassNotFoundException {

		// Vector<Pair> pairs = table.MinMax;
		// Vector<String> pages = table.serializedPages;

		for (int i = 0; i < pairs.size(); i++) {
			Pair currentPair = pairs.get(i);
			Page currentPage = null;
			FileInputStream fileIn1 = new FileInputStream(paths.get(i));
			ObjectInputStream in1 = new ObjectInputStream(fileIn1);
			currentPage = (Page) in1.readObject();
			in1.close();
			fileIn1.close();
			if (currentPage.pageVector.size() != 0) {
				// System.out.println(((Hashtable) (currentPage.pageVector.get(0))).get(pk));
				// System.out.println(((Hashtable)
				// (currentPage.pageVector.get(currentPage.pageVector.size() - 1))).get(pk));

				currentPair.setMin(((Hashtable) (currentPage.pageVector.get(0))).get(pk));
				// System.out.println(currentPair.getMin());
				currentPair.setMax(
						((Hashtable) (currentPage.pageVector.get(currentPage.pageVector.size() - 1))).get(pk));
				// System.out.println(currentPair.getMax());
			}

			FileOutputStream fileOut = new FileOutputStream(paths.get(i));
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(currentPage);
			out.close();
			fileOut.close();
		}

	}

	public int modifiedCompareTo(Object a, Object b) {
		if (a instanceof Integer) {
			if ((int) a > (int) b)
				return 1;
			else if ((int) a < (int) b)
				return -1;
			else
				return 0;
		} else if (a instanceof Double) {
			if ((double) a > (double) b)
				return 1;
			else if ((double) a < (double) b)
				return -1;
			else
				return 0;
		} else if (a instanceof String) {
			String c = (String) a;
			String d = (String) b;
			if (c.compareTo(d) < 0)
				return -1;
			else if (c.compareTo(d) > 0)
				return 1;
			else
				return 0;

		} else if (a instanceof Date) {
			Date c = (Date) a;
			Date d = (Date) b;
			if (c.compareTo(d) < 0)
				return -1;
			else if (c.compareTo(d) > 0)
				return 1;
			else
				return 0;
		}
		return 2;
	}

	public static int getIndexInsert(Vector<Object> v, Object newElementPk, Table table) { // 3amltha using binary
																							// search ya
		// seya3
		int left = 0;
		int right = v.size() - 1;
		if (newElementPk instanceof Integer) {
			// System.out.println("index check");
			while (left <= right) {
				// System.out.println("right:" + right);
				int mid = left + (right - left) / 2;
				// System.out.println("mid:" + mid);
				if (((int) (((Hashtable) v.get(mid)).get(table.primaryKey))) < (int) newElementPk)
					left = mid + 1;
				else
					right = mid - 1;
				// System.out.println("left inside while:" + left);

			}
			// System.out.println("left:" + left);
			return left;

		} else if (newElementPk instanceof Double) {
			while (left < right) {
				int mid = left + (right - left) / 2;

				if (((double) (((Hashtable) v.get(mid)).get(table.primaryKey))) < (double) newElementPk)
					left = mid + 1;
				else
					right = mid - 1;

			}
			return left;

		} else if (newElementPk instanceof String) {
			while (left < right) {
				int mid = left + (right - left) / 2;
				int result = ((String) (((Hashtable) v.get(mid)).get(table.primaryKey)))
						.compareTo((String) newElementPk);
				if (result < 0)
					left = mid + 1;
				else
					right = mid - 1;

			}
			return left;

		} else if (newElementPk instanceof Date) {
			while (left < right) {
				int mid = left + (right - left) / 2;
				int result = ((Date) (((Hashtable) v.get(mid)).get(table.primaryKey))).compareTo((Date) newElementPk);
				if (result < 0)
					left = mid + 1;
				else
					right = mid - 1;

			}
			return left;
		}
		// System.out.println("index check fail");
		return -1;
	}

	public static int getIndexUpdate(Vector<Object> v, Object newElementPk, Table table) { // 3amltha using binary
																							// search ya
		// seya3
		int left = 0;
		int right = v.size() - 1;
		if (newElementPk instanceof Integer) {
			// System.out.println("index check");
			while (left <= right) {
				int mid = left + (right - left) / 2;
				// System.out.println("mid "+ mid);
				// System.out.println(((Hashtable) v.get(mid)).get(table.primaryKey));
				// System.out.println((int) newElementPk);

				if (((int) (((Hashtable) v.get(mid)).get(table.primaryKey))) == (int) newElementPk)
					return mid;
				else if (((int) (((Hashtable) v.get(mid)).get(table.primaryKey))) < (int) newElementPk)
					left = mid + 1;
				else
					right = mid - 1;

			}
			return -1;

		} else if (newElementPk instanceof Double) {
			while (left <= right) {
				int mid = left + (right - left) / 2;
				if (((double) (((Hashtable) v.get(mid)).get(table.primaryKey))) == (double) newElementPk)
					return mid;
				else if (((double) (((Hashtable) v.get(mid)).get(table.primaryKey))) < (double) newElementPk)
					left = mid + 1;
				else
					right = mid - 1;

			}
			return -1;

		} else if (newElementPk instanceof String) {
			while (left <= right) {
				int mid = left + (right - left) / 2;
				int result = ((String) newElementPk)
						.compareTo(((String) (((Hashtable) v.get(mid)).get(table.primaryKey))));
				if (result == 0)
					return mid;
				else if (result > 0)
					left = mid + 1;
				else
					right = mid - 1;

			}
			return -1;

		} else if (newElementPk instanceof Date) {
			while (left <= right) {
				int mid = left + (right - left) / 2;
				int result = ((Date) newElementPk).compareTo(((Date) (((Hashtable) v.get(mid)).get(table.primaryKey))));
				if (result == 0)
					return mid;
				else if (result > 0)
					left = mid + 1;
				else
					right = mid - 1;

			}
			return -1;
		}
		// System.out.println("index check fail");
		return -1;
	}

	public static Object getPkFromString(String strTableName, String strClusteringKeyValue)
			throws IOException, ParseException {
		BufferedReader br = new BufferedReader(new FileReader("metadata.csv"));
		String line = "";
		while ((line = br.readLine()) != null) {
			// System.out.println("We are processing the metadata");
			String[] content = line.split(",");
			if (content[0].equals(strTableName)) {
				// System.out.println("We found the table");
				if (content[3].equals("True"))
					// System.out.println("We found the pk");
					switch (content[2]) {
						case "java.lang.Integer":
							Object result = Integer.parseInt(strClusteringKeyValue);
							// System.out.println("parsed successfully");
							return result;
						case "java.lang.Double":
							Object result1 = Double.parseDouble(strClusteringKeyValue);
							return result1;
						case "java.util.Date":
							DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
							Date date = format.parse(strClusteringKeyValue);
							return date;
						default:

							return strClusteringKeyValue;
					}
			} else
				line = br.readLine();
		}
		return strClusteringKeyValue;
	}
	// following method updates one row only
	// htblColNameValue holds the key and new value
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.

	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException, ClassNotFoundException, ParseException {
		//
		Table table = null;
		FileInputStream fileIn = new FileInputStream(
				"src/resources/tables/" + strTableName + "/" + strTableName + ".ser");
		ObjectInputStream in = new ObjectInputStream(fileIn);
		table = (Table) in.readObject();
		in.close();
		fileIn.close();
		//
		if (table.serializedPages.size() == 0) {
			throw new DBAppException();
		}
		if (htblColNameValue.get(table.Name) != null)
			throw new DBAppException();
		Object pk = getPkFromString(table.Name, strClusteringKeyValue);
		int pageIndex = getPageIndexUpdate(table.MinMax, pk);
		//
		if (pageIndex == -1)
			throw new DBAppException();
		Page currentPage = null;
		FileInputStream fileIn1 = new FileInputStream(table.serializedPages.get(pageIndex));
		ObjectInputStream in1 = new ObjectInputStream(fileIn1);
		currentPage = (Page) in1.readObject();
		in1.close();
		fileIn1.close();
		//
		// System.out.println(pk);
		// System.out.println("Update page index: " +
		// table.serializedPages.get(pageIndex));
		int tupleIndex = getIndexUpdate(currentPage.pageVector, pk, table);
		if (tupleIndex == -1)
			throw new DBAppException();
		else {
			if (checkInput(table.Name, htblColNameValue) && checkMinMax(htblColNameValue, table.Name)) {
				Hashtable tuple = (Hashtable) currentPage.pageVector.get(tupleIndex);
				for (String key : htblColNameValue.keySet()) {
					if (key.equals(table.primaryKey))
						throw new DBAppException();
					if (tuple.containsKey(key)) {
						Object newValue = htblColNameValue.get(key);
						tuple.put(key, newValue);
					}
				}
			} else
				throw new DBAppException();
		}
		FileOutputStream fileOut = new FileOutputStream(table.serializedPages.get(pageIndex));
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(currentPage);
		out.close();
		fileOut.close();

		FileOutputStream fileOut2 = new FileOutputStream(
				"src/resources/tables/" + strTableName + "/" + strTableName + ".ser");
		ObjectOutputStream out2 = new ObjectOutputStream(fileOut2);
		out2.writeObject(table);
		out2.close();
		fileOut2.close();
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException, ClassNotFoundException {
		Table table = null;
		FileInputStream fileIn = new FileInputStream(
				"src/resources/tables/" + strTableName + "/" + strTableName + ".ser");
		ObjectInputStream in = new ObjectInputStream(fileIn);
		table = (Table) in.readObject();
		in.close();
		fileIn.close();
		//

		if (table.serializedPages.size() == 0)
			return;
		if (htblColNameValue.containsKey(table.primaryKey)) {
			// System.out.println("delete specific tuple with pk");
			int pageIndex = getPageIndexUpdate(table.MinMax, htblColNameValue.get(table.primaryKey));
			// System.out.println("delete now");
			// System.out.println(pageIndex);
			// System.out.println(htblColNameValue.get(table.primaryKey));
			if (pageIndex == -1)
				throw new DBAppException();
			Page currentPage = null;
			FileInputStream fileIn1 = new FileInputStream(table.serializedPages.get(pageIndex));
			ObjectInputStream in1 = new ObjectInputStream(fileIn1);
			currentPage = (Page) in1.readObject();
			in1.close();
			fileIn1.close();
			// System.out.println("pk " + htblColNameValue.get(table.primaryKey) );
			int tupleIndex = getIndexUpdate(currentPage.pageVector, htblColNameValue.get(table.primaryKey), table);
			// System.out.println("tuple Index " + tupleIndex );
			currentPage.pageVector.remove(tupleIndex);
			FileOutputStream fileOut = new FileOutputStream(table.serializedPages.get(pageIndex));
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(currentPage);
			out.close();
			fileOut.close();
			shiftOnDelete(table);
			updatePair(table.MinMax, table.serializedPages, table.primaryKey);
			// deleteEmptyPages(table);
		}
		if (checkInput(table.Name, htblColNameValue)) {
			Vector<String> paths = table.serializedPages;
			for (String pagePath : paths) {
				Vector<Hashtable> deleteTuples = new Vector();
				Page currentPage = null;
				FileInputStream fileIn1 = new FileInputStream(pagePath);
				ObjectInputStream in1 = new ObjectInputStream(fileIn1);
				currentPage = (Page) in1.readObject();
				in1.close();
				fileIn1.close();

				Vector<Object> entries = currentPage.pageVector;
				for (Object o : entries) {
					boolean shouldDelete = true;
					Hashtable tuple = (Hashtable) o;
					for (String s : htblColNameValue.keySet()) {
						if (!checkEquality(tuple.get(s), htblColNameValue.get(s))) {
							shouldDelete = false;
							// System.out.println(tuple);
							// System.out.println(s);

						}

					}
					if (shouldDelete) {
						deleteTuples.add(tuple);
					}
					// entries.remove(o);
				}
				for (int i = 0; i < deleteTuples.size(); i++) {
					entries.remove(deleteTuples.get(i));
				}
				// deleteEmptyPages(table.Name);
				FileOutputStream fileOut = new FileOutputStream(pagePath);
				ObjectOutputStream out = new ObjectOutputStream(fileOut);
				out.writeObject(currentPage);
				out.close();
				fileOut.close();

			}
		}
		shiftOnDelete(table);
		updatePair(table.MinMax, table.serializedPages, table.primaryKey);
		deleteEmptyPages(table);
		FileOutputStream fileOut2 = new FileOutputStream(
				"src/resources/tables/" + strTableName + "/" + strTableName + ".ser");
		ObjectOutputStream out2 = new ObjectOutputStream(fileOut2);
		out2.writeObject(table);
		out2.close();
		fileOut2.close();

	}

	public static void shiftOnDelete(Table table) throws IOException, ClassNotFoundException {
		Vector<String> paths = table.serializedPages;
		for (int i = 0; i < paths.size(); i++) {
			String pagePath = paths.get(i);
			Page currentPage = null;
			FileInputStream fileIn1 = new FileInputStream(pagePath);
			ObjectInputStream in1 = new ObjectInputStream(fileIn1);
			currentPage = (Page) in1.readObject();
			in1.close();
			fileIn1.close();

			int j = i + 1;
			while (currentPage.pageVector.size() < maxRows) {
				if (j != table.serializedPages.size()) {
					Page nextPage = null;
					FileInputStream fileIn = new FileInputStream(paths.get(j));
					ObjectInputStream in = new ObjectInputStream(fileIn);
					nextPage = (Page) in.readObject();
					in.close();
					fileIn.close();
					while (currentPage.pageVector.size() < maxRows && nextPage.pageVector.size() != 0) {
						currentPage.pageVector.add(nextPage.pageVector.get(0));
						nextPage.pageVector.remove(0);
					}
					FileOutputStream fileOut = new FileOutputStream(paths.get(j));
					ObjectOutputStream out = new ObjectOutputStream(fileOut);
					out.writeObject(nextPage);
					out.close();
					fileOut.close();

					j++;
				} else
					break;

			}
			updatePair(table.MinMax, table.serializedPages, table.primaryKey);

			FileOutputStream fileOut = new FileOutputStream(pagePath);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(currentPage);
			out.close();
			fileOut.close();
			// viewTable(table.Name);
			deleteEmptyPages(table);

		}
	}

	public static void deleteEmptyPages(Table table) throws IOException, ClassNotFoundException {
		// Table table = null;
		// FileInputStream fileIn = new FileInputStream(tableName + ".ser");
		// ObjectInputStream in = new ObjectInputStream(fileIn);
		// table = (Table) in.readObject();
		// in.close();
		// fileIn.close();

		Vector<String> paths = table.serializedPages;
		for (int i = 0; i < paths.size(); i++) {
			String pagePath = paths.get(i);
			Page currentPage = null;
			FileInputStream fileIn1 = new FileInputStream(pagePath);
			ObjectInputStream in1 = new ObjectInputStream(fileIn1);
			currentPage = (Page) in1.readObject();
			in1.close();
			fileIn1.close();
			// System.out.println(pagePath + " " + currentPage.pageVector.size());
			if (currentPage.pageVector.size() == 0) {
				table.serializedPages.remove(i);
				table.MinMax.remove(i);
			}
			FileOutputStream fileOut = new FileOutputStream(pagePath);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(currentPage);
			out.close();
			fileOut.close();

		}
		// FileOutputStream fileOut = new FileOutputStream(tableName + ".ser");
		// ObjectOutputStream out = new ObjectOutputStream(fileOut);
		// out.writeObject(table);
		// out.close();
		// fileOut.close();

	}

	public static boolean checkEquality(Object a, Object b) {
		if (a instanceof Integer) {
			int c = (int) a;
			int d = (int) b;
			return d == c;
		} else if (a instanceof Double) {
			double c = (double) a;
			double d = (double) b;
			return c == d;
		} else if (a instanceof String) {
			String c = (String) a;
			String d = (String) b;
			return c.compareTo(d) == 0;
		} else if (a instanceof Date) {
			Date c = (Date) a;
			Date d = (Date) b;
			return c.compareTo(d) == 0;
		}
		return false;
	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		return null;

	}

	public static void viewTable(String strTableName) throws IOException, ClassNotFoundException {
		Table table = null;
		FileInputStream fileIn = new FileInputStream(
				"src/resources/tables/" + strTableName + "/" + strTableName + ".ser");
		ObjectInputStream in = new ObjectInputStream(fileIn);
		table = (Table) in.readObject();
		in.close();
		fileIn.close();
		table.displayPage();
	}

	public static void main(String[] args) throws DBAppException, IOException, ClassNotFoundException, ParseException {
		//// Null n = new Null();
		//// System.out.println(n);
		// String strTableName = "Student";
		// DBApp dbApp = new DBApp();
		// Hashtable htblColNameType = new Hashtable();
		// htblColNameType.put("id", "java.lang.Integer");
		// htblColNameType.put("name", "java.lang.String");
		// htblColNameType.put("gpa", "java.lang.Double");
		// Hashtable htblColNameMin = new Hashtable();
		// htblColNameMin.put("java.lang.Integer", "0");
		// htblColNameMin.put("java.lang.String", "A");
		// htblColNameMin.put("java.lang.Double", "0.0");

		// Hashtable htblColNameMax = new Hashtable();
		// htblColNameMax.put("java.lang.Integer", "9999999");
		// htblColNameMax.put("java.lang.String", "ZZZZZZZZZZZZZZZZZZZZ");
		// htblColNameMax.put("java.lang.Double", "5.0");

		// dbApp.createTable(strTableName, "id", htblColNameType, htblColNameMin,
		// htblColNameMax);
		// // //System.out.println("done creating");
		// Hashtable htblColNameValue = new Hashtable();
		// //
		// htblColNameValue.put("id", new Integer(1));
		// htblColNameValue.put("name", new String("Ahmed Maged")); // 1
		// htblColNameValue.put("gpa", new Double(0.95));
		// dbApp.insertIntoTable(strTableName, htblColNameValue);

		// htblColNameValue.clear();
		//////
		// htblColNameValue.put("id", new Integer(2));
		// htblColNameValue.put("name", new String("Ahmed hamza")); // 2
		// htblColNameValue.put("gpa", new Double(0.959));
		// dbApp.insertIntoTable(strTableName, htblColNameValue);
		//
		// htblColNameValue.clear();
		////
		// htblColNameValue.put("id", new Integer(3));
		// htblColNameValue.put("name", new String("David karam")); // 3
		// htblColNameValue.put("gpa", new Double(1.25));
		// dbApp.insertIntoTable(strTableName, htblColNameValue);
		//
		// htblColNameValue.clear();
		////
		// htblColNameValue.put("id", new Integer(5));
		// htblColNameValue.put("name", new String("John Noor")); // 4
		// htblColNameValue.put("gpa", new Double(1.5));
		// dbApp.insertIntoTable(strTableName, htblColNameValue);
		//
		// htblColNameValue.clear();
		////
		// htblColNameValue.put("id", new Integer(4));
		// htblColNameValue.put("name", new String("John Noor")); // 5
		// htblColNameValue.put("gpa", new Double(0.88));
		// dbApp.insertIntoTable(strTableName, htblColNameValue);
		//
		// htblColNameValue.clear();
		////
		////// htblColNameValue.put("id", new Integer(5674560));
		////// htblColNameValue.put("name", new String("John Noor")); // 6
		////// htblColNameValue.put("gpa", new Double(0.88));
		////// dbApp.insertIntoTable(strTableName, htblColNameValue);
		////// htblColNameValue.clear();
		////// htblColNameValue.put("id", new Integer(5674561));
		////// htblColNameValue.put("name", new String("John Noor")); // 7
		////// htblColNameValue.put("gpa", new Double(0.88));
		////// dbApp.insertIntoTable(strTableName, htblColNameValue);
		////// htblColNameValue.clear();
		////// htblColNameValue.put("id", new Integer(5674562));
		////// htblColNameValue.put("name", new String("John Noor")); // 8
		////// htblColNameValue.put("gpa", new Double(0.88));
		////// dbApp.insertIntoTable(strTableName, htblColNameValue);
		////// htblColNameValue.clear();
		////////
		////// htblColNameValue.put("id", new Integer(5674500));
		////// htblColNameValue.put("name", new String("John Noor")); // 9
		////// htblColNameValue.put("gpa", new Double(0.88));
		////// dbApp.insertIntoTable(strTableName, htblColNameValue);
		////// htblColNameValue.clear();
		////////
		////// htblColNameValue.put("id", new Integer(5));
		////// htblColNameValue.put("name", new String("John Noor")); // 10
		////// htblColNameValue.put("gpa", new Double(0.88));
		////// dbApp.insertIntoTable(strTableName, htblColNameValue);
		////// htblColNameValue.clear();
		////////
		////// htblColNameValue.put("id", new Integer(5674565));
		////// htblColNameValue.put("name", new String("John Noor")); // 11
		////// htblColNameValue.put("gpa", new Double(0.88));
		////// dbApp.insertIntoTable(strTableName, htblColNameValue);
		////// htblColNameValue.clear();
		////////
		////// htblColNameValue.put("id", new Integer(10));
		////// htblColNameValue.put("name", new String("John Noor")); // 12
		////// htblColNameValue.put("gpa", new Double(0.88));
		////// dbApp.insertIntoTable(strTableName, htblColNameValue);
		//// htblColNameValue.clear();
		////// System.out.println("update turn");
		//// htblColNameValue.put("id", new Integer("55" ) );
		//// htblColNameValue.put("name", new String("John Noor" ) );
		//// htblColNameValue.put("gpa", new Double( 1.9 ) );
		//// dbApp.updateTable(strTableName, "40", htblColNameValue);
		//// htblColNameValue.clear( );
		////////
		//////// System.out.println("update turn");
		//////// htblColNameValue.put("name", new String("John Noor" ) );
		//////// htblColNameValue.put("gpa", new Double( 1.7 ) );
		//////// dbApp.updateTable(strTableName, "78452", htblColNameValue);
		//////// htblColNameValue.clear( );
		////////
		////////
		////////
		//////// String strTableName2 = "Employee";
		//////// //DBApp dbApp = new DBApp( );
		//////// //ArrayList<Float> ColType = new ArrayList();
		//////// Hashtable htblColNameType2 = new Hashtable();
		//////// htblColNameType2.put("ssn", "java.lang.Integer");
		//////// htblColNameType2.put("name", "java.lang.String");
		//////// htblColNameType2.put("address", "java.lang.Double");
		//////// Hashtable htblColNameMin2 = new Hashtable();
		//////// htblColNameMin.put("java.lang.Integer", "0");
		//////// htblColNameMin.put("java.lang.String", "A");
		//////// htblColNameMin.put("java.lang.Double", "0.0");
		////////
		//////// Hashtable htblColNameMax2 = new Hashtable();
		//////// htblColNameMax.put("java.lang.Integer", "9999999");
		//////// htblColNameMax.put("java.lang.String", "ZZZZZZZZZZZZZZZZZZZZ");
		//////// htblColNameMax.put("java.lang.Double", "5.0");
		//////
		//////
		////// //dbApp.createTable( strTableName2, "ssn", htblColNameType2
		////// ,htblColNameMin2,htblColNameMax2);
		////////
		////// htblColNameValue.clear( );
		////// htblColNameValue.put("name", new String("John wick" ) );
		////// htblColNameValue.put("gpa", new Double( 1.9 ) );
		////// dbApp.deleteFromTable(strTableName, htblColNameValue);
		// htblColNameValue.clear( );
		// //htblColNameValue.put("id", new Integer("1" ) );
		// htblColNameValue.put("name", new String("John Noor" ) );
		// htblColNameValue.put("gpa", new Double( 0.88 ) );
		// dbApp.deleteFromTable(strTableName, htblColNameValue);
		//////
		//////// htblColNameValue.put("id", new Integer("5674565" ) );
		//////// dbApp.deleteFromTable(strTableName, htblColNameValue);
		//////
		//////// htblColNameValue.put("id", new Integer("5674500" ) );
		//////// dbApp.deleteFromTable(strTableName, htblColNameValue);
		////////
		//////// htblColNameValue.put("id", new Integer("5674560" ) );
		//////// dbApp.deleteFromTable(strTableName, htblColNameValue);
		//////////
		//////
		////// // htblColNameValue.put("id", new Integer( 453458 ));
		////// // htblColNameValue.put("name", new String("Ahmed Noor" ) );
		////// // htblColNameValue.put("gpa", new Double( 0.95 ) );
		////// dbApp.insertIntoTable(
		////// // strTableName , htblColNameValue );
		// viewTable(strTableName);
		// Table table = null;
		// FileInputStream fileIn = new FileInputStream("src/resources/tables/" +
		// strTableName +"/"+ strTableName +".ser");
		// ObjectInputStream in = new ObjectInputStream(fileIn);
		// table = (Table) in.readObject();
		// in.close();
		// fileIn.close();
		// for(int i = 0;i<table.MinMax.size();i++)
		// System.out.println(table.MinMax.get(i));

	}
}
