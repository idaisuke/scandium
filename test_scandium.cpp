#define BOOST_TEST_NO_LIB
#define BOOST_TEST_NO_MAIN
#define BOOST_TEST_MODULE test_scandium

#include <boost/lexical_cast.hpp>
#include <boost/test/included/unit_test.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include "scandium.h"

namespace {
    std::string db_root_path = "./";
    std::string create_random_name() {
        boost::uuids::random_generator gen;
        return db_root_path + boost::lexical_cast<std::string>(gen());
    }
}

int test_scandium() {
    return test_scandium(0, nullptr);
}

int test_scandium(const std::string db_root_path) {
    ::db_root_path = db_root_path;
    return test_scandium(0, nullptr);
}

int test_scandium(int argc, char **argv) {
    extern ::boost::unit_test::test_suite *init_unit_test_suite(int, char **);

    boost::unit_test::init_unit_test_func init_func = &init_unit_test_suite;

    if (argc && argv) {
        return ::boost::unit_test::unit_test_main(init_func, argc, argv);
    } else {
        const char *argv2[] = {"test_scandium"};
        return ::boost::unit_test::unit_test_main(init_func, 1, const_cast<char **>(argv2));
    }
}

BOOST_AUTO_TEST_CASE(open_close) {
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

BOOST_AUTO_TEST_CASE(exec_sql_query) {
    scandium::database db(create_random_name());
    db.open();

    db.exec_sql("CREATE TABLE table_1(id INTEGER, name TEXT);");
    db.exec_sql("INSERT INTO table_1 VALUES(2, 'name 1');");
    db.exec_sql("INSERT INTO table_1 VALUES(4, 'name 2');");

    db.exec_sql("INSERT INTO table_1 VALUES(?, ?);", 6, "name 3");
    db.exec_sql("INSERT INTO table_1 VALUES(?, ?);", 8, "name 4");

    {
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

    {
        auto result = db.query("SELECT id FROM table_1 WHERE id < ?;", 7);
        auto it = result.begin();
        for (int i = 1; i <= 3; ++i) {
            auto id = it->get<int>("id");
            BOOST_CHECK_EQUAL(i * 2, id);
            ++it;
        }

        BOOST_CHECK_EQUAL(it == result.end(), true);
        BOOST_CHECK_EQUAL(it != result.end(), false);
    }
}

BOOST_AUTO_TEST_CASE(transaction) {
    auto modes = {
            scandium::transaction_mode::deferred,
            scandium::transaction_mode::immediate,
            scandium::transaction_mode::exclusive,
    };

    for (auto &&mode : modes) {
        scandium::database db(create_random_name());
        db.open();

        db.exec_sql("CREATE TABLE table_1(id INTEGER, name TEXT);");
        try {
            auto transaction = db.create_transaction(mode);
            db.exec_sql("INSERT INTO table_1 VALUES(1, 'name 1');");
            db.exec_sql("INSERT INTO table_1 VALUES(2, 'name 2');");
            transaction.commit();
        } catch (...) {

        }

        try {
            auto transaction = db.create_transaction(mode);
            db.exec_sql("INSERT INTO table_1 VALUES(100, 'name 100');");
            db.exec_sql("INSERT INTO xxxxxxx VALUES(200, 'name 200');");
            db.exec_sql("INSERT INTO table_1 VALUES(300, 'name 300');");
            transaction.commit();
        } catch (...) {

        }

        {
            db.begin_transaction(mode);
            db.exec_sql("INSERT INTO table_1 VALUES(3, 'name 3');");
            db.exec_sql("INSERT INTO table_1 VALUES(4, 'name 4');");
            db.commit_transaction();

            db.begin_transaction(mode);
            db.exec_sql("INSERT INTO table_1 VALUES(400, 'name 400');");
            db.exec_sql("INSERT INTO table_1 VALUES(500, 'name 500');");
            db.rollback_transaction();
        }

        auto result = db.query("SELECT id FROM table_1;");
        auto it = result.begin();
        for (int i = 1; i <= 4; ++i) {
            auto id = it->get<int>("id");
            BOOST_CHECK_EQUAL(i, id);
            ++it;
        }

        BOOST_CHECK_EQUAL(it == result.end(), true);
        BOOST_CHECK_EQUAL(it != result.end(), false);
    }
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

BOOST_AUTO_TEST_CASE(statement) {
    scandium::database db(create_random_name());
    db.open();
    db.exec_sql("CREATE TABLE table_1(id INTEGER, data);");

    std::vector<unsigned char> orig_vec = {
            'a', 'b', 'c', '\0', 'd', 'e', 'f', 'g', '\0',
    };

    scandium::blob orig_blob{
            .size = static_cast<int>(orig_vec.size()),
            .data = orig_vec.data(),
    };

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

        statement.bind(1, 999);
        statement.bind(2, 999);
        statement.bind_values(6, 200);
        statement.exec();

        statement.bind(1, 999);
        statement.bind(2, 999);
        statement.exec_with_bindings(7, "777");

        statement.exec_with_bindings(8, std::string("888"));

        std::string str999("999");
        statement.exec_with_bindings(9, str999);

        statement.bind(1, 10);
        statement.bind(2, orig_vec);
        statement.exec();

        statement.bind(1, 11);
        statement.bind(2, orig_blob);
        statement.exec();

        statement.bind(1, 12);
        statement.bind(2, orig_blob.data, orig_blob.size);
        statement.exec();

        statement.exec_with_bindings(13, orig_vec);

        statement.exec_with_bindings(14, orig_blob);

        statement.bind(1, 15);
        statement.bind(2, 1);
        statement.exec();

        statement.bind(1, 16);
        statement.bind(2, nullptr);
        statement.exec();

        statement.exec_with_bindings(17, 1);

        statement.exec_with_bindings(18, nullptr);

        statement.finalize();

        BOOST_CHECK_THROW(statement.bind(1, 20), std::logic_error);
        BOOST_CHECK_THROW(statement.exec(), std::logic_error);
    }

    {
        auto statement = db.prepare_statement("INSERT INTO table_1 VALUES(:id, :data);");
        statement.bind(":id", 101);
        statement.bind(":data", 101101);
        statement.exec();
    }

    auto stmt = db.prepare_statement("SELECT data FROM table_1;");
    auto result = stmt.query();
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
    BOOST_CHECK_EQUAL(std::strcmp(it->get<const char *>(0), "string"), 0);
    ++it;

    BOOST_CHECK_EQUAL(it->get<std::string>(0), std::string("const char *"));
    BOOST_CHECK_EQUAL(std::strcmp(it->get<const char *>(0), "const char *"), 0);
    ++it;

    BOOST_CHECK_EQUAL(it->get<int>(0), 200);
    ++it;

    BOOST_CHECK_EQUAL(it->get<std::string>(0), std::string("777"));
    ++it;

    BOOST_CHECK_EQUAL(it->get<std::string>(0), std::string("888"));
    ++it;

    BOOST_CHECK_EQUAL(it->get<std::string>(0), std::string("999"));
    ++it;

    for (int i = 0; i < 5; ++i) {
        auto vec = it->get<std::vector<unsigned char>>(0);
        BOOST_CHECK_EQUAL_COLLECTIONS(vec.begin(), vec.end(), orig_vec.begin(), orig_vec.end());
        auto blob = it->get<scandium::blob>(0);
        auto data = reinterpret_cast<const unsigned char *>(blob.data);
        BOOST_CHECK_EQUAL_COLLECTIONS(data, data + blob.size, orig_vec.begin(), orig_vec.end());
        ++it;
    }

    BOOST_CHECK_EQUAL(it->is_null(0), false);
    BOOST_CHECK_EQUAL(it->is_null("data"), false);
    ++it;

    BOOST_CHECK_EQUAL(it->is_null(0), true);
    BOOST_CHECK_EQUAL(it->is_null("data"), true);
    ++it;

    BOOST_CHECK_EQUAL(it->is_null(0), false);
    BOOST_CHECK_EQUAL(it->is_null("data"), false);
    ++it;

    BOOST_CHECK_EQUAL(it->is_null(0), true);
    BOOST_CHECK_EQUAL(it->is_null("data"), true);
    ++it;

    BOOST_CHECK_EQUAL(it->get<int>(0), 101101);
    ++it;

    BOOST_CHECK_EQUAL(it == result.end(), true);
    BOOST_CHECK_EQUAL(it != result.end(), false);

    {
        auto statement = db.prepare_statement("SELECT data FROM table_1 WHERE id = ?;");
        int count = 0;
        for (auto &&cursor : statement.query_with_bindings(7)) {
            ++count;
            BOOST_CHECK_EQUAL(cursor.get<std::string>(0), std::string("777"));
        }
        BOOST_CHECK_EQUAL(count, 1);
    }

    {
        auto statement = db.prepare_statement("SELECT data FROM table_1 WHERE id = ?123;");
        statement.bind(123, 7);
        int count = 0;
        for (auto &&cursor : statement.query()) {
            ++count;
            BOOST_CHECK_EQUAL(cursor.get<std::string>(0), std::string("777"));
        }
        BOOST_CHECK_EQUAL(count, 1);
    }
}

#ifdef SQLITE_HAS_CODEC
BOOST_AUTO_TEST_CASE(sqlcipher) {
    auto path = create_random_name();
    scandium::database db(path);
    db.open("12345");
    db.exec_sql("CREATE TABLE table_1(id INTEGER, data);");
    db.exec_sql("INSERT INTO table_1 VALUES(1, 'data 1');");
    db.exec_sql("INSERT INTO table_1 VALUES(2, 'data 2');");
    db.close();

    db.open();
    BOOST_CHECK_THROW(db.query("SELECT id FROM table_1;"), scandium::sqlite_error);
    db.close();

    db.open("abcde");
    BOOST_CHECK_THROW(db.query("SELECT id FROM table_1;"), scandium::sqlite_error);
    db.close();

    db.open("12345");
    auto result = db.query("SELECT id FROM table_1;");
    auto it = result.begin();
    for (int i = 1; i <= 2; ++i) {
        auto id = it->get<int>("id");
        BOOST_CHECK_EQUAL(i, id);
        ++it;
    }
    db.close();
}
#endif
