#define BOOST_TEST_MAIN

#include <boost/test/included/unit_test.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include "scandium.h"

std::string create_random_name() {
    boost::uuids::random_generator gen;
    return "./" + boost::lexical_cast<std::string>(gen());
}

BOOST_AUTO_TEST_CASE(open) {
    auto path = create_random_name();
    scandium::database db(path);
    BOOST_CHECK_EQUAL(db.is_open(), false);
    db.open();
    BOOST_CHECK_EQUAL(db.is_open(), true);
    db.close();
    BOOST_CHECK_EQUAL(db.is_open(), false);
    db.open();
    BOOST_CHECK_EQUAL(db.is_open(), true);
    db.close();

    auto copied = db;
    BOOST_CHECK_EQUAL(db.is_open(), false);
    copied.open();
    BOOST_CHECK_EQUAL(db.is_open(), true);
    copied.close();
    BOOST_CHECK_EQUAL(db.is_open(), false);
    copied.open();
    BOOST_CHECK_EQUAL(db.is_open(), true);
    copied.close();

    scandium::database db2(path);
    BOOST_CHECK_EQUAL(db2.is_open(), false);
    db2.open();
    BOOST_CHECK_EQUAL(db2.is_open(), true);
    db2.close();
    BOOST_CHECK_EQUAL(db2.is_open(), false);
    db2.open();
    BOOST_CHECK_EQUAL(db2.is_open(), true);
    db2.close();
}

BOOST_AUTO_TEST_CASE(query) {
    scandium::database db(create_random_name());
    db.open();

    db.exec_sql("CREATE TABLE table_1(id INTEGER, name TEXT);");
    db.exec_sql("INSERT INTO table_1 VALUES(2, 'name 1');");
    db.exec_sql("INSERT INTO table_1 VALUES(4, 'name 2');");

    db.exec_sql("INSERT INTO table_1 VALUES(?, ?);", 6, "name 3");
    db.exec_sql("INSERT INTO table_1 VALUES(?, ?);", 8, "name 4");

    auto result = db.query("SELECT id FROM table_1;");
    auto it = result.begin();
    for (int i = 1; i <= 4; ++i) {
        auto id = it->get<int>("id");
        BOOST_CHECK_EQUAL(i * 2, id);
        ++it;
    }

    BOOST_CHECK_EQUAL(it == result.end(), true);
    BOOST_CHECK_EQUAL(it != result.end(), false);
}

BOOST_AUTO_TEST_CASE(transaction) {
    scandium::database db(create_random_name());
    db.open();

    db.exec_sql("CREATE TABLE table_1(id INTEGER, name TEXT);");
    try {
        auto transaction = db.create_transaction();
        db.exec_sql("INSERT INTO table_1 VALUES(1, 'name 1');");
        db.exec_sql("INSERT INTO table_1 VALUES(2, 'name 2');");
        transaction.commit();
    } catch (...) {

    }

    try {
        auto transaction = db.create_transaction();
        db.exec_sql("INSERT INTO table_1 VALUES(3, 'name 3');");
        db.exec_sql("INSERT INTO xxxxxxx VALUES(4, 'name 4');");
        db.exec_sql("INSERT INTO table_1 VALUES(5, 'name 5');");
        transaction.commit();
    } catch (...) {

    }

    auto result = db.query("SELECT id FROM table_1;");
    auto it = result.begin();
    for (int i = 1; i <= 2; ++i) {
        auto id = it->get<int>("id");
        BOOST_CHECK_EQUAL(i, id);
        ++it;
    }

    BOOST_CHECK_EQUAL(it == result.end(), true);
    BOOST_CHECK_EQUAL(it != result.end(), false);
}

BOOST_AUTO_TEST_CASE(user_version) {
    auto path = create_random_name();
    scandium::database db(path);
    db.open();

    BOOST_CHECK_EQUAL(db.get_user_version(), 0);
    db.update_user_version(1);
    BOOST_CHECK_EQUAL(db.get_user_version(), 1);
    db.close();

    scandium::database db2(path);
    db2.open();
    BOOST_CHECK_EQUAL(db2.get_user_version(), 1);

    db2.set_before_upgrade_user_version([](scandium::database *db, int old_version, int new_version) {
        BOOST_CHECK_EQUAL(old_version, 1);
        BOOST_CHECK_EQUAL(new_version, 3);
    });
    db2.update_user_version(3);
    BOOST_CHECK_EQUAL(db2.get_user_version(), 3);

    db2.set_before_downgrade_user_version([](scandium::database *db, int old_version, int new_version) {
        BOOST_CHECK_EQUAL(old_version, 3);
        BOOST_CHECK_EQUAL(new_version, 2);
    });
    db2.update_user_version(2);
    BOOST_CHECK_EQUAL(db2.get_user_version(), 2);
}

BOOST_AUTO_TEST_CASE(bind) {
    scandium::database db(create_random_name());
    db.open();
    db.exec_sql("CREATE TABLE table_1(id INTEGER, data);");

    {
        auto statement = db.prepare_statement("INSERT INTO table_1 VALUES(?, ?);");
        statement.bind(1, 1);
        statement.bind(2, 100);
        statement.exec();

        statement.bind(1, 2);
        statement.bind(2, static_cast<sqlite3_int64>(10000));
        statement.exec();

        statement.bind(1, 3);
        statement.bind(2, 55.55);
        statement.exec();

        statement.bind(1, 4);
        statement.bind(2, std::string("string"));
        statement.exec();

        statement.bind(1, 5);
        statement.bind(2, "const char *");
        statement.exec();
    }

    auto result = db.query("SELECT data FROM table_1;");
    auto it = result.begin();

    BOOST_CHECK_EQUAL(it->get<int>(0), 100);
    ++it;

    BOOST_CHECK_EQUAL(it->get<sqlite3_int64>(0), 10000);
    ++it;

    BOOST_CHECK_EQUAL(it->get<int>(0), 55);
    BOOST_CHECK_EQUAL(it->get<double>(0), 55.55);
    BOOST_CHECK_EQUAL(it->get<std::string>(0), std::string("55.55"));
    ++it;

    BOOST_CHECK_EQUAL(it->get<std::string>(0), std::string("string"));
    BOOST_CHECK_EQUAL(std::strcmp(it->get<const char *>(0), "string") , 0);
    ++it;

    BOOST_CHECK_EQUAL(it->get<std::string>(0), std::string("const char *"));
    BOOST_CHECK_EQUAL(std::strcmp(it->get<const char *>(0), "const char *") , 0);
    ++it;

    BOOST_CHECK_EQUAL(it == result.end(), true);
    BOOST_CHECK_EQUAL(it != result.end(), false);
}

void test_scandium() {
    scandium::database db("./scandium.db");

#ifdef SQLITE_HAS_CODEC
    db.open("12345678");
#else
    db.open();
#endif

    db.set_before_upgrade_user_version([](scandium::database *db, int old_version, int new_version) {
        if (old_version <= 0) {
            db->exec_sql("CREATE TABLE chara(id INTEGER, name TEXT);");

            auto stmt = db->prepare_statement("INSERT INTO chara VALUES(?, ?);");

            stmt.exec_with_bindings(1, "キャラX");
            stmt.exec_with_bindings(2, "キャラY");
            stmt.exec_with_bindings(4, "キャラZ");
        }
    });
    db.update_user_version(1);

    for (auto &&cursor : db.query("SELECT * FROM chara;")) {
        auto id = cursor.get<int>("id");
        auto name = cursor.get<const char *>("name");

        printf("XXXXX id = %d, name = %s\n", id, name);
    }

    auto select = db.prepare_statement("SELECT * FROM chara WHERE id >= ?;");
    for (auto &&cursor : select.query_with_bindings(2)) {
        auto id = cursor.get<int>("id");
        auto name = cursor.get<const char *>("name");

        printf("YYYYY id = %d, name = %s\n", id, name);
    }
}
