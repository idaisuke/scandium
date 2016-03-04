#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#define BOOST_TEST_MODULE scandium

#include <boost/lexical_cast.hpp>
//#include <boost/test/unit_test.hpp>
#include <boost/test/included/unit_test.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include "scandium.h"

std::string create_random_name() {
    boost::uuids::random_generator gen;
    return "./" + boost::lexical_cast<std::string>(gen());
}

bool is_exeption_thrown(std::function<void ()> action) {
    auto ret = false;
    try {
        action();
    } catch (...) {
        ret = true;
    }
    return ret;
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

    db.set_busy_timeout(1000);

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

    scandium::blob orig_blob {
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

        statement.exec_with_bindings(10, orig_vec);

        statement.exec_with_bindings(11, orig_blob);

        statement.bind(1, 12);
        statement.bind(2, 1);
        statement.exec();

        statement.bind(1, 13);
        statement.bind(2, nullptr);
        statement.exec();

        statement.exec_with_bindings(14, 1);

        statement.exec_with_bindings(15, nullptr);

        statement.finalize();

        auto is_error = is_exeption_thrown([&]{
            statement.bind(1, 20);
            statement.bind(2, 1);
            statement.exec();
        });

        BOOST_CHECK_EQUAL(is_error, true);
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

    {
        auto vec = it->get<std::vector<unsigned char>>(0);
        BOOST_CHECK_EQUAL_COLLECTIONS(vec.begin(), vec.end(), orig_vec.begin(), orig_vec.end());
        auto blob = it->get<scandium::blob>(0);
        auto data = reinterpret_cast<const unsigned char *>(blob.data);
        BOOST_CHECK_EQUAL_COLLECTIONS(data, data + blob.size, orig_vec.begin(), orig_vec.end());
        ++it;
    }

    {
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

}
#endif
