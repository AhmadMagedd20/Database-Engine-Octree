package Miliestone1;

public class SQLTerm {
	String _strTableName;
	String _strColumnName;
	String _strOperator;
	Object _objValue;
	
	public String getStrTableName() {
		return _strTableName;
	}

	public String getStrColumnName() {
		return _strColumnName;
	}

	public String getStrOperator() {
		return _strOperator;
	}

	public Object getObjValue() {
		return _objValue;
	}

	public void setStrTableName(String strTableName) {
		this._strTableName = strTableName;
	}

	public void setStrColumnName(String strColumnName) {
		this._strColumnName = strColumnName;
	}

	public void setStrOperator(String strOperator) {
		this._strOperator = strOperator;
	}

	public void setObjValue(Object objValue) {
		this._objValue = objValue;
	}

	public SQLTerm(String strTableName, String strColumnName, String strOperator, Object objValue) {
		super();
		this._strTableName = strTableName;
		this._strColumnName = strColumnName;
		this._strOperator = strOperator;
		this._objValue = objValue;
	}

}
