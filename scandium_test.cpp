#include <iostream>
#include "scandium.h"

using namespace std;

void test_scandium() {


    scandium::database db("./scandium_test.db");

    db.open();
    //db.open("12345678");
    db.set_before_upgrade_user_version([](scandium::database *db, int old_version, int new_version) {
        if (old_version <= 0) {
            db->exec_sql("CREATE TABLE chara(id INTEGER, name TEXT);");
        }
    });
    db.update_user_version(1);

    {
        auto transaction = db.create_transaction();

        auto stmt = db.prepare_statement("INSERT INTO chara VALUES(?, ?);");

        stmt.exec_with_bindings(1, "キャラX");
        stmt.exec_with_bindings(2, "キャラY");
        stmt.exec_with_bindings(4, "キャラZ");

        transaction.commit();
    }

    for (auto &&cursor : db.query("SELECT * FROM chara;")) {
        auto id = cursor.get<int>("id");
        auto name = cursor.get<const char *>("name");

        printf("XXXXX id = %d, name = %s", id, name);
    }


    cout << "Hello, World!" << endl;
}
