package Miliestone1;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

public class Table implements Serializable {
	Vector<String> serializedPages;
	Vector<Pair> MinMax;
	String Name;
	String primaryKey;
	public static int pageCounter = 1;

	public Table(String Name, String primaryKey) {
		this.serializedPages = new Vector();
		this.Name = Name;
		this.primaryKey = primaryKey;
		this.MinMax = new Vector();
	}

	public void displayPage() throws IOException, ClassNotFoundException {
		for (int i = 0; i < serializedPages.size(); i++) {
			System.out.println(serializedPages.get(i));
			Page currentPage = null;
			FileInputStream fileIn2 = new FileInputStream(serializedPages.get(i));
			ObjectInputStream in2 = new ObjectInputStream(fileIn2);
			currentPage = (Page) in2.readObject();
			in2.close();
			fileIn2.close();
			for (int j = 0; j < currentPage.pageVector.size(); j++) {
				System.out.println(currentPage.pageVector.get(j));
			}
			System.out.println("-------------------------------------------------------");
		}

		System.out.println("-------------------------------------------------------");

		for (int i = 0; i < MinMax.size(); i++) {
			System.out.println(MinMax.get(i));
		}

		System.out.println(this.serializedPages.size());
	}

}
