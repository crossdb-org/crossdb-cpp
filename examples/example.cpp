#include <iostream>
#include "../crossdb.hpp"

using namespace std;

int main ()
{
	auto conn = CrossDB::Driver::connect(":memory:");

	try {
		conn->execute ("CREATE TABLE student (id INT PRIMARY KEY, name CHAR(16), age INT, class CHAR(16))");
		auto count = conn->executeUpdate ("INSERT INTO student (id,name,age,class) VALUES (1001,'jack',10,'3-1'), (1002,'tom',11,'2-5')");
		cout << "insert rows: " << count << '\n';

		auto res = conn->executeQuery ("SELECT * FROM student");
		cout << "[select *: " << res->rowsCount() << "]\n";
		while (res->next()) {
			cout << "  id: " 	<< res->getInt("id");
			cout << "  name: " 	<< res->getString("name");
			cout << "  age: " 	<< res->getInt("age");
			cout << "  class: " << res->getString("class") << endl;
		}
		auto meta = res->getMetaData ();
		cout << "Meta:" << endl;
		for (int i = 0; i < meta->getColumnCount(); ++i) {
			cout << "  " << meta->getColumnName(i) << ':' << meta->getColumnType(i) << ':' << meta->getColumnTypeName(i) << endl;
		}

		res = conn->executeQuery ("SELECT COUNT(*) as cnt FROM student");
		while (res->next()) {
			cout << "COUNT(*): " << res->getInt("cnt") << endl;
		}
		meta = res->getMetaData ();
		cout << "Meta:" << endl;
		for (int i = 0; i < meta->getColumnCount(); ++i) {
			cout << "  " << meta->getColumnName(i) << ':' << meta->getColumnType(i) << ':' << meta->getColumnTypeName(i) << endl;
		}

		auto stmt = conn->createStatement ();
		auto res1 = stmt->executeQuery ("SELECT * FROM student WHERE id=1001");
		auto res2 = stmt->executeQuery ("SELECT * FROM student WHERE id=1002");

		cout << "[select 1001: " << res1->rowsCount () << "]" << endl;
		while (res1->next()) {
			cout << "  id: " 	<< res1->getInt(0);
			cout << "  name: " 	<< res1->getString(1);
			cout << "  age: " 	<< res1->getInt(2);
			cout << "  class: " << res1->getString(3) << endl;
		}

		cout << "[select 1002: " << res2->rowsCount() << "]" << endl;
		while (res2->next()) {
			cout << "  id: " 	<< res2->getInt(0);
			cout << "  name: " 	<< res2->getString(1);
			cout << "  age: " 	<< res2->getInt(2);
			cout << "  class: " << res2->getString(3) << endl;
		}

		auto pstmt = conn->createPreparedStatement ("SELECT * FROM student WHERE id=?");
		pstmt->setInt (1, 1002);
		auto res3 = pstmt->executeQuery ();
		cout << "[pstmt 1002: " << res3->rowsCount() << "]" << endl;
		while (res3->next()) {
			cout << "  id: " 	<< res3->getInt(0);
			cout << "  name: " 	<< res3->getString(1);
			cout << "  age: " 	<< res3->getInt(2);
			cout << "  class: " << res3->getString(3) << endl;
		}
	} catch (CrossDB::SQLException &e) {
		cout << e.what() << " : "  << e.getSql() << " : " << e.getErrorCode() << " : " << e.getErrorMsg() << endl;
	}

	return 0;
}
