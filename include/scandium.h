// The MIT License (MIT)
//
// Copyright (c) 2016 Daisuke Itabashi (itabashi.d@gmail.com)
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#pragma once

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <sstream>
#include <string>
#include <thread>
#include <utility>

#include "sqlite3.h"

//TODO: blobをtextからblobに先頭バイトにサイズを埋め込む?
//TODO: blob64, null

namespace scandium {

    class database;

    class iterator;

    class result_set;

    /**
     *  Represents a BLOB object of SQLite.
     */
    struct blob {
        const void *data;
        int size;
    };

    /**
     *  Represents a 64bit BLOB object of SQLite.
     */
    struct blob64 {
        const void *data;
        sqlite3_uint64 size;
    };

    /**
     *  Describes the SQLite transaction modes.
     */
    enum class transaction_mode {
        deferred,
        immediate,
        exclusive,
    };

    /**
     *  Represents an exception that is thrown when SQLite error occurred.
     */
    class sqlite_error : public std::runtime_error {
    public:
        sqlite_error(const char *what, int rc);

        sqlite_error(const std::string &what, int rc);

        int result_code() const;

    private:
        const int _rc;
    };

    /**
     *  A wrapper for sqlite3 using the RAII idiom.
     */
    class sqlite_holder {
    public:
        /*
         *  Constructor.
         */
        sqlite_holder(sqlite3 *db);

        /**
         *  Destructor.
         *  Closes the underlying sqlite3 handle if not closed, do nothing otherwise.
         */
        ~sqlite_holder() noexcept;

        /**
         *  Closes the underlying sqlite3 handle.
         */
        void close();

        /**
         *  Returns true if the underlying sqlite3 is closed, false otherwise.
         */
        bool is_closed() const;

        /**
         *  Executes the SQL statement that does not return data.
         */
        void exec_sql(const std::string &sql);

        /**
         *  Begins a transaction.
         *
         *  @param mode transaction mode
         */
        void begin_transaction(transaction_mode mode = transaction_mode::deferred);

        /**
         *  Commits a transaction.
         */
        void commit_transaction();

        /**
         *  Rollbacks a transaction.
         */
        void rollback_transaction();

        /**
         *  Returns the underlying sqlite3 handle.
         */
        sqlite3 *handle() const;

    private:
        sqlite3 *_db;
    };

    /**
     *  A wrapper for sqlite3_stmt using the RAII idiom.
     */
    class sqlite_stmt_holder {
    public:
        /*
         *  Constructor.
         */
        sqlite_stmt_holder(sqlite3_stmt *stmt) noexcept;

        /**
         *  Destructor.
         *  Finalizes the underlying sqlite3_stmt handle if not finalized, do nothing otherwise.
         */
        ~sqlite_stmt_holder() noexcept;

        /**
         *  Finalizes the underlying sqlite3_stmt handle.
         */
        void finalize();

        /**
         *  Returns true if the underlying sqlite3_stmt handle is finalized, false otherwise.
         */
        bool is_finalized() const;

        void step();

        void bind_values(int);

        template<class... ArgType>
        void bind_values(int index, int first_arg, ArgType &&... bind_args);

        template<class... ArgType>
        void bind_values(int index, sqlite3_int64 first_arg, ArgType &&... bind_args);

        template<class... ArgType>
        void bind_values(int index, double first_arg, ArgType &&... bind_args);

        template<class... ArgType>
        void bind_values(int index, std::string first_arg, ArgType &&... bind_args);

        template<class... ArgType>
        void bind_values(int index, const char *first_arg, ArgType &&... bind_args);

        template<class... ArgType>
        void bind_values(int index, blob first_arg, ArgType &&... bind_args);

        /**
         *  Returns the underlying sqlite3_stmt handle.
         */
        sqlite3_stmt *handle() const;

    private:
        sqlite3_stmt *_stmt;
    };

    /**
     *  Represents a precompiled SQL statement.
     */
    class statement {
    public:

        /**
         *  Move constructor.
         */
        statement(statement &&other) noexcept;

        /**
         *  Move assignment operator.
         */
        statement &operator=(statement &&other) noexcept;

        /**
         *  Finalizes this statement.
         */
        void finalize();

        /**
         *  Executes this statement.
         */
        void exec();

        /**
         *  Executes this statement.
         *  Removes all bindings that were given before
         *  and rebind the arguments to the placeholders before the execution.
         */
        template<class... ArgType>
        void exec_with_bindings(ArgType &&... bind_args);

        /**
         *  Executes this statement and returns a result set.
         */
        result_set query();

        /**
         *  Executes this statement and returns a result set.
         *  Removes all bindings that were given before
         *  and rebind the arguments to the placeholders before the execution.
         */
        template<class... ArgType>
        result_set query_with_bindings(ArgType &&... bind_args);

        /**
         *  Binds the values to the placeholders such as ?.
         *
         *  @param bind_args the values to bind to the placeholders.
         */
        template<class... ArgType>
        void bind_values(ArgType &&... bind_args);

        /**
         *  Binds the value to the placeholder such as ? or ?NNN (NNN represents an integer literal).
         *
         *  @param index the one-based index or the index equivalent to NNN.
         *  @param value the value to bind to the placeholder.
         */
        void bind(int index, int value);

        /**
         *  @copydoc statement::bind(int,int)
         */
        void bind(int index, sqlite3_int64 value);

        /**
         *  @copydoc statement::bind(int,int)
         */
        void bind(int index, double value);

        /**
         *  @copydoc statement::bind(int,int)
         */
        void bind(int index, const std::string &value);

        /**
         *  @copydoc statement::bind(int,int)
         */
        void bind(int index, const char *value);

        /**
         *  @copydoc statement::bind(int,int)
         */
        void bind(int index, blob value);

        /**
         *  Binds the value to the placeholder such as :VVV, @VVV or $VVV (VVV represents an alphanumeric identifier).
         *
         *  @param parameter_name the parameter name equivalent to VVV
         *  @param value          the value to bind to the placeholder
         */
        void bind(const std::string &parameter_name, int value);

        /**
         *  @copydoc statement::bind(const std::string &,int)
         */
        void bind(const std::string &parameter_name, sqlite3_int64 value);

        /**
         *  @copydoc statement::bind(const std::string &,int)
         */
        void bind(const std::string &parameter_name, double value);

        /**
         *  @copydoc statement::bind(const std::string &,int)
         */
        void bind(const std::string &parameter_name, const std::string &value);

        /**
         *  @copydoc statement::bind(const std::string &,int)
         */
        void bind(const std::string &parameter_name, const char *value);

        /**
         *  @copydoc statement::bind(const std::string &,int)
         */
        void bind(const std::string &parameter_name, blob value);

        /**
         *  Removes all binding values.
         */
        void clear_bindings();

    private:
        statement(std::shared_ptr<sqlite_holder> db_holder, const std::string &sql);

        statement(const statement &) = delete;

        statement &operator=(const statement &) = delete;

        std::shared_ptr<sqlite_holder> _db_holder;
        std::shared_ptr<sqlite_stmt_holder> _stmt_holder;

        friend class database;

        friend class result_set;
    };

    /**
     *  Represents a cursor to fetch data from a row.
     */
    class cursor {
    public:
        /**
         *  Gets data from the current row.
         *
         *  @tparam T int, sqlite3_int64, double, std::string, const char *, const void * or fe::blob.
         *
         *  @param column_index the zero-based column index.
         */
        template<class T>
        T get(int column_index) const;

        /**
         *  Gets data for the given column name from the current row,
         *  or throws an exception if the column name does not exist.
         *
         *  @tparam T int, sqlite3_int64, double, std::string, const char *, const void * or fe::blob.
         *
         *  @param column_name the column name.
         */
        template<class T>
        T get(const std::string &column_name) const;

        /**
         *  Returns the column name at the given zero-based column index.
         *
         *  @param column_index the zero-based index.
         */
        std::string get_column_name(int column_index) const;

        /**
         *  Returns the zero-based index for the given column name, or -1 if the column does not exist.
         *
         *  @param column_name the name of the target column.
         */
        int get_column_index(const char *column_name) const;

        /**
         *  @copydoc cursor::get_column_index(const char *)
         */
        int get_column_index(const std::string &column_name) const;

        /**
         *  Returns total number of columns.
         */
        int get_column_count() const;

    private:
        cursor(const std::shared_ptr<sqlite_stmt_holder> &stmt_holder);

        std::shared_ptr<sqlite_stmt_holder> _stmt_holder;

        friend class iterator;
    };

    /**
     *  Provides an STL-style forward iterator to fetch data from SQLite result set.
     */
    class iterator : public std::iterator<std::forward_iterator_tag, cursor> {
    public:
        iterator(iterator &&other) noexcept;

        iterator &operator=(iterator &&other) noexcept;

        iterator &operator++();

        cursor &operator*();

        cursor *operator->();

        bool operator==(const iterator &other) const;

        bool operator!=(const iterator &other) const;

    private:
        iterator();

        iterator(
                const std::shared_ptr<sqlite_holder> &db_holder,
                const std::shared_ptr<sqlite_stmt_holder> &stmt_holder,
                std::uint_fast64_t row_index,
                int state);

        iterator(const iterator &) = delete;

        iterator &operator=(const iterator &) = delete;

        std::shared_ptr<sqlite_holder> _db_holder;
        std::shared_ptr<sqlite_stmt_holder> _stmt_holder;
        cursor _cursor;
        std::uint_fast64_t _row_index;
        int _state;

        friend class result_set;
    };

    /**
     *  Represents an SQLite result set.
     */
    class result_set {
    public:
        /**
         *  Returns the iterator pointing to the first row.
         *
         *  @attention The iterator got in the past becomes invalid.
         */
        iterator begin();

        /**
         *  Returns the iterator to end.
         */
        iterator end();

    private:
        result_set(
                const std::shared_ptr<sqlite_holder> &db_holder,
                const std::shared_ptr<sqlite_stmt_holder> &stmt_holder);

        std::shared_ptr<sqlite_holder> _db_holder;
        std::shared_ptr<sqlite_stmt_holder> _stmt_holder;

        friend class statement;

        friend class database;
    };

    /**
     *  Represents a transaction using the RAII idiom.
     */
    class transaction {
    public:
        /**
         *  Destructor.
         *  Rollbacks this transaction if not committed, or does nothing if committed.
         */
        ~transaction() noexcept;

        /**
         *  Move constructor.
         */
        transaction(transaction &&other) noexcept;

        /**
         *  Move assignment operator.
         */
        transaction &operator=(transaction &&other) noexcept;

        /**
         *  Commits this transaction.
         */
        void commit();

    private:
        /**
         *  Constructor.
         *  Begin a transaction.
         */
        transaction(const std::shared_ptr<sqlite_holder> &db_holder, transaction_mode mode);

        transaction(const transaction &) = delete;

        transaction &operator=(const transaction &) = delete;

        std::shared_ptr<sqlite_holder> _db_holder;
        bool _in_transaction;

        friend class database;
    };

    /**
     *  Used for receiving notifications when the user version of the SQLite database has changed and so on.
     */
    struct sqlite_listener {
        /**
         *  Called when the SQLite database needs to be upgraded.
         */
        std::function<void(database *db, int old_version, int new_version)> on_upgrade;

        /**
         *  Called when the SQLite database needs to be downgraded.
         */
        std::function<void(database *db, int old_version, int new_version)> on_downgrade;
    };

    /**
     *  Represents an SQLite database.
     */
    class database {
    public:
        /**
         *  Constructor to create an in-memory database.
         */
        database();

        /**
         *  Constructor.
         *
         *  @param path the path of the SQLite database file to open and/or create.
         */
        database(const std::string &path);

        /**
         *  Move constructor.
         */
        database(database &&other) noexcept;

        /**
         *  Move assignment operator.
         */
        database &operator=(database &&other) noexcept;

        /**
         *  Opens the database and/or creates the SQLite database file.
         */
        void open();

#ifdef SQLITE_HAS_CODEC

        /**
         *  Opens the database and/or creates the SQLite database file.
         *
         *  @param passphrase the passphrase to encrypt the database if using sqlcipher and so on.
         */
        void open(const std::string &passphrase);

#endif

        /**
         *  Closes this database.
         */
        void close();

        /**
         *  Executes the SQL statement that does not return data.
         */
        void exec_sql(const std::string &sql);

        /**
         *  Executes the SQL statement that does not return data.
         *
         *  @param sql       the single SQL statement.
         *  @param bind_args the values to bind to the placeholders such as ?
         */
        template<class... ArgType>
        void exec_sql(const std::string &sql, ArgType &&... bind_args);

        /**
         *  Runs the given SQL statement that returns data such as SELECT.
         */
        result_set query(const std::string &sql);

        /**
         *  Runs the given SQL statement that returns data such as SELECT.
         *
         *  @param sql       the single SQL statement.
         *  @param bind_args the values to bind to the placeholders such as ?
         */
        template<class... ArgType>
        result_set query(const std::string &sql, ArgType &&... bind_args);

        /**
         *  Creates a precompiled SQL statement.
         */
        statement prepare_statement(const std::string &sql);

        /**
         *  Begins a transaction.
         *
         *  @param mode the transaction mode.
         */
        void begin_transaction(transaction_mode mode = transaction_mode::deferred);

        /**
         *  Commits a transaction.
         */
        void commit_transaction();

        /**
         *  Rollbacks a transaction.
         */
        void rollback_transaction();

        /**
         *  Returns true if the database is open, or false otherwise.
         */
        bool is_open() const;

        /**
         *  Begins a transaction, and Returns a RAII object.
         */
        transaction create_transaction(transaction_mode mode = transaction_mode::deferred);

        /**
         *  Gets the user version of the database, or 0 that is a default value.
         */
        int get_user_version();

        /**
         *  Updates the user version of the database in a transaction.
         *
         *  @param version the new version that must be > 0.
         *  @param mode    the transaction mode.
         */
        void update_user_version(int version, transaction_mode mode = transaction_mode::deferred);

        /**
         *  Sets a callback to be called before an upgrade user version.
         *
         *  @tparam CallbackType void(*)(database *db, int old_version, int new_version)
         *
         *  @param callback the callback to be called before an upgrade user version.
         */
        template<class CallbackType>
        void set_before_upgrade_user_version(CallbackType &&callback);

        /**
         *  Returns the file path of the database, or ":memory:" if the in-memory database.
         */
        const std::string &get_path() const;

    private:
        database(const database &) = delete;

        database &operator=(const database &) = delete;

        std::string _path;
        std::shared_ptr<sqlite_holder> _db_holder;
        std::function<void(database *, int, int)> _before_upgrade_user_version;

        friend class statement;

        friend class iterator;
    };

#pragma mark ## sqlite_error ##

    inline sqlite_error::sqlite_error(const char *what, int rc) : std::runtime_error(what), _rc(rc) {
    }

    inline sqlite_error::sqlite_error(const std::string &what, int rc) : std::runtime_error(what), _rc(rc) {
    }

    inline int sqlite_error::result_code() const {
        return _rc;
    }

#pragma mark ## sqlite_holder ##

    inline sqlite_holder::sqlite_holder(sqlite3 *db) : _db(db) {
    }

    inline sqlite_holder::~sqlite_holder() noexcept {
        if (_db) {
            sqlite3_close_v2(_db);
        }
    }

    inline void sqlite_holder::close() {
        if (!_db) {
            return;
        }

        auto rc = sqlite3_close_v2(_db);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to close database", rc);
        }

        _db = nullptr;
    }

    inline bool sqlite_holder::is_closed() const {
        return _db == nullptr;
    }

    inline void sqlite_holder::exec_sql(const std::string &sql) {
        sqlite3_stmt *stmt;
        auto rc = sqlite3_prepare_v2(handle(), sql.c_str(), static_cast<int>(sql.length()), &stmt, nullptr);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to prepare statement, SQL = \"" + sql + "\"", rc);
        }

        rc = sqlite3_step(stmt);
        if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
            throw sqlite_error("Failed to step statement", rc);
        }

        rc = sqlite3_finalize(stmt);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to finalize statement", rc);
        }
    }

    inline void sqlite_holder::begin_transaction(transaction_mode mode) {
        const char *sql;
        switch (mode) {
            case transaction_mode::deferred:
                sql = "BEGIN DEFERRED;";
                break;

            case transaction_mode::immediate:
                sql = "BEGIN IMMEDIATE;";
                break;

            case transaction_mode::exclusive:
                sql = "BEGIN EXCLUSIVE;";
                break;
        }

        exec_sql(sql);
    }

    inline void sqlite_holder::commit_transaction() {
        exec_sql("COMMIT;");
    }

    inline void sqlite_holder::rollback_transaction() {
        exec_sql("ROLLBACK;");
    }

    inline sqlite3 *sqlite_holder::handle() const {
        if (_db == nullptr) {
            throw std::logic_error("database is closed");
        }
        return _db;
    }

#pragma mark ## sqlite_stmt_holder ##

    inline sqlite_stmt_holder::sqlite_stmt_holder(sqlite3_stmt *stmt) noexcept : _stmt(stmt) {
    }

    inline sqlite_stmt_holder::~sqlite_stmt_holder() noexcept {
        if (_stmt) {
            sqlite3_finalize(_stmt);
        }
    }

    inline void sqlite_stmt_holder::finalize() {
        auto rc = sqlite3_finalize(_stmt);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to finalize statement", rc);
        }
        _stmt = nullptr;
    }

    inline bool sqlite_stmt_holder::is_finalized() const {
        return _stmt == nullptr;
    }

    inline void sqlite_stmt_holder::step() {
        auto rc = sqlite3_step(handle());
        if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
            throw sqlite_error("Failed to step statement", rc);
        }
    }

    inline void sqlite_stmt_holder::bind_values(int) {
    }

    template<class... ArgType>
    void sqlite_stmt_holder::bind_values(int index, int first_arg, ArgType &&... bind_args) {
        auto rc = sqlite3_bind_int(handle(), index, first_arg);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to bind int", rc);
        }
        bind_values(index + 1, std::forward<ArgType>(bind_args)...);
    }

    template<class... ArgType>
    void sqlite_stmt_holder::bind_values(int index, sqlite3_int64 first_arg, ArgType &&... bind_args) {
        auto rc = sqlite3_bind_int64(handle(), index, first_arg);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to bind int64", rc);
        }
        bind_values(index + 1, std::forward<ArgType>(bind_args)...);
    }

    template<class... ArgType>
    void sqlite_stmt_holder::bind_values(int index, double first_arg, ArgType &&... bind_args) {
        auto rc = sqlite3_bind_double(handle(), index, first_arg);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to bind double", rc);
        }
        bind_values(index + 1, std::forward<ArgType>(bind_args)...);
    }

    template<class... ArgType>
    void sqlite_stmt_holder::bind_values(int index, std::string first_arg, ArgType &&... bind_args) {
        auto rc = sqlite3_bind_text(handle(), index, first_arg.c_str(), static_cast<int>(first_arg.length()),
                SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to bind text", rc);
        }
        bind_values(index + 1, std::forward<ArgType>(bind_args)...);
    }

    template<class... ArgType>
    void sqlite_stmt_holder::bind_values(int index, const char *first_arg, ArgType &&... bind_args) {
        auto rc = sqlite3_bind_text(handle(), index, first_arg, static_cast<int>(std::strlen(first_arg)),
                SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to bind text", rc);
        }
        bind_values(index + 1, std::forward<ArgType>(bind_args)...);
    }

    template<class... ArgType>
    void sqlite_stmt_holder::bind_values(int index, blob first_arg, ArgType &&... bind_args) {
        auto rc = sqlite3_bind_blob(handle(), index, first_arg.data, first_arg.size, SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to bind blob", rc);
        }
        bind_values(index + 1, std::forward<ArgType>(bind_args)...);
    }

    inline sqlite3_stmt *sqlite_stmt_holder::handle() const {
        if (_stmt == nullptr) {
            throw std::logic_error("statement is finalized");
        }
        return _stmt;
    }

#pragma mark ## statement ##

    inline statement::statement(statement &&other) noexcept
            : _db_holder(std::move(other._db_holder)), _stmt_holder(std::move(other._stmt_holder)) {
    }

    inline statement &statement::operator=(statement &&other) noexcept {
        _db_holder = std::move(other._db_holder);
        _stmt_holder = std::move(other._stmt_holder);

        return *this;
    }

    inline void statement::finalize() {
        _stmt_holder->finalize();
    }

    inline void statement::exec() {
        _stmt_holder->step();
    }

    template<class... ArgType>
    void statement::exec_with_bindings(ArgType &&... bind_args) {
        clear_bindings();
        bind_values(std::forward<ArgType>(bind_args)...);
        exec();
        finalize();
    }

    inline result_set statement::query() {
        return result_set(_db_holder, _stmt_holder);
    }

    template<class... ArgType>
    result_set statement::query_with_bindings(ArgType &&... bind_args) {
        clear_bindings();
        bind_values(std::forward<ArgType>(bind_args)...);
        return query();
    }

    template<class... ArgType>
    void statement::bind_values(ArgType &&... bind_args) {
        _stmt_holder->bind_values(1, std::forward<ArgType>(bind_args)...);
    }

    inline void statement::bind(int index, int value) {
        auto rc = sqlite3_bind_int(_stmt_holder->handle(), index, value);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to bind int", rc);
        }
    }

    inline void statement::bind(int index, sqlite3_int64 value) {
        auto rc = sqlite3_bind_int64(_stmt_holder->handle(), index, value);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to bind int64", rc);
        }
    }

    inline void statement::bind(int index, double value) {
        auto rc = sqlite3_bind_double(_stmt_holder->handle(), index, value);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to bind double", rc);
        }
    }

    inline void statement::bind(int index, const std::string &value) {
        auto rc = sqlite3_bind_text(_stmt_holder->handle(), index, value.c_str(), static_cast<int>(value.length()), SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to bind text", rc);
        }
    }

    inline void statement::bind(int index, const char *value) {
        auto rc = sqlite3_bind_text(_stmt_holder->handle(), index, value, static_cast<int>(std::strlen(value)), SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to bind text", rc);
        }
    }

    inline void statement::bind(int index, blob value) {
        auto rc = sqlite3_bind_blob(_stmt_holder->handle(), index, value.data, value.size, SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to bind blob", rc);
        }
    }

    inline void statement::bind(const std::string &parameter_name, int value) {
        auto index = sqlite3_bind_parameter_index(_stmt_holder->handle(), parameter_name.c_str());
        if (index == 0) {
            std::string what;
            what.append("No matching parameter named '");
            what.append(parameter_name);
            what.append("' is found");
            throw std::logic_error(what);
        }

        bind(index, value);
    }

    inline void statement::bind(const std::string &parameter_name, sqlite3_int64 value) {
        auto index = sqlite3_bind_parameter_index(_stmt_holder->handle(), parameter_name.c_str());
        if (index == 0) {
            std::string what;
            what.append("No matching parameter named '");
            what.append(parameter_name);
            what.append("' is found");
            throw std::logic_error(what);
        }

        bind(index, value);
    }

    inline void statement::bind(const std::string &parameter_name, double value) {
        auto index = sqlite3_bind_parameter_index(_stmt_holder->handle(), parameter_name.c_str());
        if (index == 0) {
            std::string what;
            what.append("No matching parameter named '");
            what.append(parameter_name);
            what.append("' is found");
            throw std::logic_error(what);
        }

        bind(index, value);
    }

    inline void statement::bind(const std::string &parameter_name, const std::string &value) {
        auto index = sqlite3_bind_parameter_index(_stmt_holder->handle(), parameter_name.c_str());
        if (index == 0) {
            std::string what;
            what.append("No matching parameter named '");
            what.append(parameter_name);
            what.append("' is found");
            throw std::logic_error(what);
        }

        bind(index, value);
    }

    inline void statement::bind(const std::string &parameter_name, const char *value) {
        auto index = sqlite3_bind_parameter_index(_stmt_holder->handle(), parameter_name.c_str());
        if (index == 0) {
            std::string what;
            what.append("No matching parameter named '");
            what.append(parameter_name);
            what.append("' is found");
            throw std::logic_error(what);
        }

        bind(index, value);
    }

    inline void statement::bind(const std::string &parameter_name, blob value) {
        auto index = sqlite3_bind_parameter_index(_stmt_holder->handle(), parameter_name.c_str());
        if (index == 0) {
            std::string what;
            what.append("No matching parameter named '");
            what.append(parameter_name);
            what.append("' is found");
            throw std::logic_error(what);
        }

        bind(index, std::move(value));
    }

    inline void statement::clear_bindings() {
        auto rc = sqlite3_clear_bindings(_stmt_holder->handle());
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to clear bindings", rc);
        }
    }

    inline statement::statement(std::shared_ptr<sqlite_holder> db_holder, const std::string &sql)
            : _db_holder(std::move(db_holder)) {
        sqlite3_stmt *stmt;
        auto rc = sqlite3_prepare_v2(_db_holder->handle(), sql.c_str(), static_cast<int>(sql.length()), &stmt, nullptr);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to prepare statement, SQL = \"" + sql + "\"", rc);
        }
        _stmt_holder = std::make_shared<sqlite_stmt_holder>(stmt);
    }

#pragma mark ## cursor ##

    template<>
    inline int cursor::get(int column_index) const {
        return sqlite3_column_int(_stmt_holder->handle(), column_index);
    }

    template<>
    inline sqlite3_int64 cursor::get(int column_index) const {
        return sqlite3_column_int64(_stmt_holder->handle(), column_index);
    }

    template<>
    inline double cursor::get(int column_index) const {
        return sqlite3_column_double(_stmt_holder->handle(), column_index);
    }

    template<>
    inline const unsigned char *cursor::get(int column_index) const {
        return sqlite3_column_text(_stmt_holder->handle(), column_index);
    }

    template<>
    inline const char *cursor::get(int column_index) const {
        return reinterpret_cast<const char *>(sqlite3_column_text(_stmt_holder->handle(), column_index));
    }

    template<>
    inline std::string cursor::get(int column_index) const {
        return reinterpret_cast<const char *>(sqlite3_column_text(_stmt_holder->handle(), column_index));
    }

    template<>
    inline const void *cursor::get(int column_index) const {
        return sqlite3_column_blob(_stmt_holder->handle(), column_index);
    }

    template<>
    inline blob cursor::get(int column_index) const {
        blob blob;
        blob.data = sqlite3_column_text(_stmt_holder->handle(), column_index);
        blob.size = static_cast<int>(std::strlen(reinterpret_cast<const char *>(blob.data)) - 1);
        return blob;
    }

    template<class T>
    T cursor::get(const std::string &column_name) const {
        auto index = get_column_index(column_name);
        if (index == -1) {
            std::string what;
            what.append("Column named '");
            what.append(column_name);
            what.append("' does not exist");
            throw std::logic_error(what);
        }
        return get<T>(index);
    }

    inline std::string cursor::get_column_name(int column_index) const {
        return sqlite3_column_name(_stmt_holder->handle(), column_index);
    }

    inline int cursor::get_column_index(const char *column_name) const {
        for (int i = 0, n = sqlite3_column_count(_stmt_holder->handle()); i < n; ++i) {
            if (0 == std::strcmp(sqlite3_column_name(_stmt_holder->handle(), i), column_name)) {
                return i;
            }
        }
        return -1;
    }

    inline int cursor::get_column_index(const std::string &column_name) const {
        return get_column_index(column_name.c_str());
    }

    inline int cursor::get_column_count() const {
        return sqlite3_column_count(_stmt_holder->handle());
    }

    inline cursor::cursor(const std::shared_ptr<sqlite_stmt_holder> &stmt_holder)
            : _stmt_holder(stmt_holder) {
    }

#pragma mark ## iterator ##

    inline iterator::iterator(iterator &&other) noexcept : _db_holder(std::move(other._db_holder)),
                                                                                _stmt_holder(std::move(other._stmt_holder)),
                                                                                _cursor(std::move(other._cursor)),
                                                                                _row_index(other._row_index),
                                                                                _state(other._state) {
    }

    inline iterator &iterator::operator=(iterator &&other) noexcept {
        _db_holder = std::move(other._db_holder);
        _stmt_holder = std::move(other._stmt_holder);
        _cursor = std::move(other._cursor);
        _row_index = other._row_index;
        _state = other._state;

        return *this;
    }

    inline iterator &iterator::operator++() {
        _state = sqlite3_step(_stmt_holder->handle());

        if (_state != SQLITE_ROW && _state != SQLITE_DONE) {
            throw sqlite_error("Failed to step statement", _state);
        }

        _row_index++;
        return *this;
    }

    inline cursor &iterator::operator*() {
        return _cursor;
    }

    inline cursor *iterator::operator->() {
        return &_cursor;
    }

    inline bool iterator::operator==(const iterator &other) const {
        if (_state == SQLITE_DONE && other._state == -1) {
            return true;
        }

        if (_state == -1 && other._state == SQLITE_DONE) {
            return true;
        }

        return _db_holder == other._db_holder
                && _stmt_holder == other._stmt_holder
                && _row_index == other._row_index
                && _state == other._state;
    }

    inline bool iterator::operator!=(const iterator &other) const {
        return !operator==(other);
    }

    inline iterator::iterator()
            : _db_holder(), _stmt_holder(), _cursor(nullptr), _row_index(0), _state(-1) {
    }

    inline iterator::iterator(
            const std::shared_ptr<sqlite_holder> &db_holder,
            const std::shared_ptr<sqlite_stmt_holder> &stmt_holder,
            std::uint_fast64_t row_index,
            int state)
            : _db_holder(db_holder),
              _stmt_holder(stmt_holder),
              _cursor(stmt_holder),
              _row_index(row_index),
              _state(state) {
    }

#pragma mark ## result_set ##

    inline iterator result_set::begin() {
        auto rc = sqlite3_reset(_stmt_holder->handle());
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to reset statement", rc);
        }

        rc = sqlite3_step(_stmt_holder->handle());
        if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
            throw sqlite_error("Failed to step statement", rc);
        }

        return iterator(_db_holder, _stmt_holder, 0, rc);
    }

    inline iterator result_set::end() {
        return iterator();
    }

    inline result_set::result_set(
            const std::shared_ptr<sqlite_holder> &db_holder,
            const std::shared_ptr<sqlite_stmt_holder> &stmt_holder)
            : _db_holder(db_holder), _stmt_holder(stmt_holder) {
    }

#pragma mark ## transaction ##

    inline transaction::~transaction() noexcept {
        if (_in_transaction && !_db_holder->is_closed()) {
            try {
                _db_holder->rollback_transaction();
            } catch (...) {
                // ignore
            }
        }
    }

    inline transaction::transaction(transaction &&other) noexcept
            : _db_holder(std::move(other._db_holder)),
              _in_transaction(other._in_transaction) {
        other._in_transaction = false;
    }

    inline transaction &transaction::operator=(transaction &&other) noexcept {
        _db_holder = std::move(other._db_holder);
        _in_transaction = other._in_transaction;
        other._in_transaction = false;

        return *this;
    }

    inline void transaction::commit() {
        _db_holder->commit_transaction();
        _in_transaction = false;
    }

    inline transaction::transaction(
            const std::shared_ptr<sqlite_holder> &db_holder,
            transaction_mode mode)
            : _db_holder(db_holder) {
        _db_holder->begin_transaction(mode);
        _in_transaction = true;
    }

#pragma mark ## database ##

    inline database::database() : _path(":memory:") {
    }

    inline database::database(const std::string &path) : _path(path) {
    }

    inline database::database(database &&other) noexcept : _path(std::move(other._path)),
                                                                                _db_holder(std::move(other._db_holder)) {
    }

    inline database &database::operator=(database &&other) noexcept {
        _path = std::move(other._path);
        _db_holder = std::move(other._db_holder);
        return *this;
    }

    inline void database::open() {
        sqlite3 *db;
        auto rc = sqlite3_open_v2(_path.c_str(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);
        if (rc != SQLITE_OK) {
            sqlite3_close(db);
            throw sqlite_error("Failed to open database", rc);
        }
        _db_holder = std::make_shared<sqlite_holder>(db);
    }

#ifdef SQLITE_HAS_CODEC

    inline void database::open(const std::string &passphrase) {
        open();
        sqlite3_key(_db_holder->handle(), passphrase.c_str(), static_cast<int>(passphrase.length()));
    }

#endif

    inline void database::close() {
        _db_holder->close();
    }

    inline void database::exec_sql(const std::string &sql) {
        _db_holder->exec_sql(sql);
    }

    template<class... ArgType>
    void database::exec_sql(const std::string &sql, ArgType &&... bind_args) {
        statement statement(_db_holder, sql);
        statement.bind_values(std::forward<ArgType>(bind_args)...);
        statement.exec();
        statement.finalize();
    }

    inline result_set database::query(const std::string &sql) {
        statement statement(_db_holder, sql);
        return statement.query();
    }

    template<class... ArgType>
    result_set database::query(const std::string &sql, ArgType &&... bind_args) {
        statement statement(_db_holder, sql);
        statement.bind_values(std::forward<ArgType>(bind_args)...);
        return statement.query();
    }

    inline statement database::prepare_statement(const std::string &sql) {
        return statement(_db_holder, sql);
    }

    inline void database::begin_transaction(transaction_mode mode) {
        _db_holder->begin_transaction(mode);
    }

    inline void database::commit_transaction() {
        _db_holder->commit_transaction();
    }

    inline void database::rollback_transaction() {
        _db_holder->rollback_transaction();
    }

    inline bool database::is_open() const {
        return _db_holder.get() == nullptr;
    }

    inline transaction database::create_transaction(transaction_mode mode) {
        return transaction(_db_holder, mode);
    }

    inline int database::get_user_version() {
        auto &&results = query("PRAGMA user_version;");

        int result = 0;
        for (auto &&cursor : results) {
            result = cursor.get<int>(0);
        }

        return result;
    }

    inline void database::update_user_version(int version, transaction_mode mode) {
        if (version < 1) {
            throw std::logic_error("Invalid version, must be > 0");
        }

        auto old_version = get_user_version();
        if (old_version == version) {
            return;
        }

        auto transaction = create_transaction(mode);

        if (_before_upgrade_user_version) {
            _before_upgrade_user_version(this, old_version, version);
        }

        std::stringstream ss;
        ss << "PRAGMA user_version = " << version << ";";
        exec_sql(ss.str());

        transaction.commit();
    }

    template<class CallbackType>
    void database::set_before_upgrade_user_version(CallbackType &&callback) {
        _before_upgrade_user_version = std::forward<CallbackType>(callback);
    }

    inline const std::string &database::get_path() const {
        return _path;
    }
}
