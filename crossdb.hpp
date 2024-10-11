/******************************************************************************
* Copyright (c) 2024-present JC Wang. All rights reserved
*
*   https://crossdb.org
*   https://github.com/crossdb-org/crossdb
*
* This Source Code Form is subject to the terms of the MIT License
******************************************************************************/

#include <crossdb.h>
#include <memory>
#include <unordered_map>

using std::string;
using std::unordered_map;
using std::unique_ptr;

namespace CrossDB {
	class SQLException : public std::exception {
	public:
		SQLException (const string & sql, int errCode, const string & errMsg) {
			this->sql = sql;
			this->errCode = errCode;
			this->errMsg = errMsg;
		}

		int getErrorCode () const {
			return this->errCode;
		}

		const string & getErrorMsg () const {
			return this->errMsg;
		}

		const string & getSql () const {
			return this->sql;
		}

	protected:
		int 	errCode;
		string 	errMsg;
		string 	sql;
	};

	class ResultSetMetaData {
	public:
		ResultSetMetaData () {
			this->metaData = 0;
		}
		~ResultSetMetaData () {}

		void setMetaData (uint64_t metaData) {
			this->metaData = metaData;
		}

		uint16_t getColumnCount () {
			return ((xdb_meta_t*)metaData)->col_count;
		}
		string getColumnName (uint16_t iCol) {
			return xdb_column_name (this->metaData, iCol);
		}
		xdb_type_t getColumnType (uint16_t iCol) {
			return xdb_column_type (this->metaData, iCol);
		}
		const string getColumnTypeName (uint16_t iCol) {
			return xdb_type2str ((xdb_type_t)getColumnType (iCol));
		}

	private:
		uint64_t metaData;
	};

	class ResultSet {
	public:
		ResultSet () {
			this->pRes = NULL;
			this->col_meta = 0;
		}
		ResultSet (xdb_res_t *pRes) {
			this->pRes = NULL;
			this->col_meta = 0;
			setRes (pRes);
		}

		~ResultSet () {
			this->close ();
		}
		void setRes (xdb_res_t *pRes) {
			if (this->pRes) {
				this->close ();
			}
			this->pRes = pRes;
			this->col_meta = pRes->col_meta;
			this->meta.setMetaData (pRes->col_meta);
		}

		void close () {
			if (NULL != this->pRes) {
				if (this->pRes->col_meta > 0) {
					xdb_free_result (this->pRes);
				}
				this->pRes = nullptr;
			}
			if (this->col2idMap.size()) {
				this->col2idMap.clear ();	
			}
		}
		bool next () {
			if (this->pRes) {
				this->pRow = xdb_fetch_row (this->pRes);
				if (NULL == this->pRow) {
					this->close ();
				}
				return this->pRow != nullptr;
			}
			return false;
		}

		uint16_t getColId (const string & name) {
			if (0 == this->col2idMap.size()) {
				if (this->pRes && this->pRes->col_meta) {
					for (int i = 0; i < this->pRes->col_count; ++i) {
						xdb_col_t *pCol = xdb_column_meta(this->pRes->col_meta, i);
						this->col2idMap.emplace (pCol->col_name, i+1);
					}
				}
			}
			return this->col2idMap[name] - 1;
		}

		uint64_t rowsCount () {
			return this->pRes ? this->pRes->row_count : 0;
		}
		uint64_t getUpdateCount () {
			return this->pRes ? this->pRes->affected_rows : 0;
		}

		int getInt (uint16_t iCol) {
			return xdb_column_int (this->col_meta, this->pRow, iCol);
		}
		int getInt (const string & name) {
			return xdb_column_int (this->col_meta, this->pRow, getColId(name));
		}
		int64_t getInt64 (uint16_t iCol) {
			return xdb_column_int64 (this->col_meta, this->pRow, iCol);
		}
		const string getString (uint16_t iCol) {
			return xdb_column_str (this->col_meta, this->pRow, iCol);
		}
		const string getString (const string & name) {
			return xdb_column_str (this->col_meta, this->pRow, getColId(name));
		}
		float getFloat (uint16_t iCol) {
			return xdb_column_float (this->col_meta, this->pRow, iCol);
		}
		double getDouble (uint16_t iCol) {
			return xdb_column_double (this->col_meta, this->pRow, iCol);
		}

		ResultSetMetaData * getMetaData() {
			return &meta;
		}

	private:
		xdb_res_t	*pRes;
		xdb_row_t	*pRow;
		ResultSetMetaData	meta;
		uint64_t	col_meta;
		unordered_map <string, uint16_t>	col2idMap;
	};

	class Statement {
	public:
		Statement () {
			this->pConn = NULL;
		}
		Statement (xdb_conn_t *pConn) {
			setConn (pConn);
		}
		~Statement() {
			this->close ();
		};
		void setConn (xdb_conn_t *pConn) {
			this->pConn = pConn;
		}

		void close() {
			this->res.close ();
		}
		bool execute (const string & sql) {
			xdb_res_t *pRes = xdb_exec (this->pConn, sql.c_str());
			if (pRes->errcode != XDB_OK) {
				throw SQLException (sql, pRes->errcode, xdb_errmsg(pRes));
			}
			this->res.setRes (pRes);
			return pRes->col_count > 0;
		}
		int executeUpdate (const string & sql) {
			xdb_res_t *pRes = xdb_exec (this->pConn, sql.c_str());
			if (pRes->errcode != XDB_OK) {
				throw SQLException (sql, pRes->errcode, xdb_errmsg(pRes));
			}
			this->res.setRes (pRes);
			return static_cast<int>(pRes->affected_rows);
		}
		unique_ptr<ResultSet> executeQuery (const string & sql) {
			xdb_res_t *pRes = xdb_exec (this->pConn, sql.c_str());
			if (pRes->errcode != XDB_OK) {
				throw SQLException (sql, pRes->errcode, xdb_errmsg(pRes));
			}
			unique_ptr<ResultSet> pResObj (new ResultSet(pRes));
			return pResObj;
		}

		bool getMoreResults() {
			return false;
		}
		uint64_t getUpdateCount() {
			return res.getUpdateCount ();
		};
		bool begin () {
			return XDB_OK == xdb_begin (this->pConn);
		}
		bool commit () {
			return XDB_OK == xdb_commit (this->pConn);
		}
		bool rollback () {
			return XDB_OK == xdb_rollback (this->pConn);
		}

	protected:
		xdb_conn_t	*pConn;
		ResultSet	res;
	};

	class PreparedStatement : public Statement
	{
	public:
		PreparedStatement (xdb_conn_t *pConn, const string & sql) {
			pStmt = xdb_stmt_prepare (pConn, sql.c_str());
			this->sql = sql;
		};
		~PreparedStatement() {
			xdb_stmt_close (this->pStmt);
		}
		bool execute () {
			xdb_res_t *pRes = xdb_stmt_exec (this->pStmt);
			if (pRes->errcode != XDB_OK) {
				throw SQLException (sql, pRes->errcode, xdb_errmsg(pRes));
			}
			this->res.setRes (pRes);
			return pRes->col_count > 0;
		}
		int executeUpdate () {
			xdb_res_t *pRes = xdb_stmt_exec (this->pStmt);
			if (pRes->errcode != XDB_OK) {
				throw SQLException (sql, pRes->errcode, xdb_errmsg(pRes));
			}
			this->res.setRes (pRes);
			return static_cast<int>(pRes->affected_rows);
		}
		unique_ptr<ResultSet> executeQuery () {
			xdb_res_t *pRes = xdb_stmt_exec (this->pStmt);
			if (pRes->errcode != XDB_OK) {
				throw SQLException (sql, pRes->errcode, xdb_errmsg(pRes));
			}
			unique_ptr<ResultSet> pResObj (new ResultSet(pRes));
			return pResObj;
		}

		void clearParameters() {
			xdb_clear_bindings (this->pStmt);
		}
		void setDouble (uint16_t paraId, double value) {
			xdb_bind_double (this->pStmt, paraId, value);
		}
		void setInt (uint16_t paraId, int value) {
			xdb_bind_int (this->pStmt, paraId, value);
		}
		void setInt64 (uint16_t paraId, int64_t value) {
			xdb_bind_int64 (this->pStmt, paraId, value);
		}
		void setString (uint16_t paraId, const string & value) {
			xdb_bind_str2 (this->pStmt, paraId, value.c_str(), value.length());
		}
	private:
		xdb_stmt_t * pStmt;
		string sql;
	};

	class Connection : public Statement {
	public:
		Connection (xdb_conn_t	*pConn) {
			this->pConn = pConn;
			setConn (pConn);
		}
		~Connection () {
			this->close ();
		}
		void close () {
			Statement::close ();
			xdb_close (this->pConn);
			this->pConn = NULL;
		}
		unique_ptr<Statement> createStatement () {
			unique_ptr<Statement> pStmtObj (new Statement(this->pConn));
			return pStmtObj;
		}
		unique_ptr<PreparedStatement> createPreparedStatement (const string & sql) {
			unique_ptr<PreparedStatement> pStmtObj (new PreparedStatement(this->pConn, sql));
			return pStmtObj;
		}

	private:
		xdb_conn_t	*pConn;
	};

	class Driver {
	public:
		static unique_ptr<Connection> connect (const string & dbname) {
			xdb_conn_t	*pXdbConn = xdb_open (dbname.c_str());
			unique_ptr<Connection> pConnObj (new Connection(pXdbConn));
			return pConnObj;
		}
	};
};
