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
//TODO: blob64
//TODO: exec_sqlとqueryの統合
//TODO: sqlite_error -> sqlite_error & std::runtime_error

namespace scandium {

    class sqlite_database;

    class sqlite_iterator;

    class sqlite_query;

    /**
     *  Represents a BLOB object of SQLite.
     */
    struct sqlite_blob {
        const void *data;
        int size;
    };

    /**
     *  Represents a 64bit BLOB object of SQLite.
     */
    struct sqlite_blob64 {
        const void *data;
        sqlite3_uint64 size;
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
         *  Closes sqlite3 if not closed.
         */
        ~sqlite_holder() noexcept;

        /**
         *  Closes sqlite3.
         */
        void close();

        /**
         *  Returns sqlite3 handle.
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
         *  Finalizes sqlite3_stmt if not finalized.
         */
        ~sqlite_stmt_holder() noexcept;

        /**
         *  Finalizes sqlite3_stmt.
         */
        void finalize();

        /**
         *  Returns sqlite3_stmt handle.
         */
        sqlite3_stmt *handle() const;

    private:
        sqlite3_stmt *_stmt;
    };

    /**
     *  Represents a precompiled SQL statement.
     */
    class sqlite_statement {
    public:

        /**
         *  Move constructor.
         */
        sqlite_statement(sqlite_statement &&other) noexcept;

        /**
         *  Move assignment operator.
         */
        sqlite_statement &operator=(sqlite_statement &&other) noexcept;

        /**
         *  Finalizes this statement.
         */
        void finalize();

        /**
         *  TODO:
         */
        void exec();

        /**
         *  TODO:
         */
        sqlite_query query();

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
         *  @copydoc sqlite_statement::bind(int,int)
         */
        void bind(int index, sqlite3_int64 value);

        /**
         *  @copydoc sqlite_statement::bind(int,int)
         */
        void bind(int index, double value);

        /**
         *  @copydoc sqlite_statement::bind(int,int)
         */
        void bind(int index, const std::string &value);

        /**
         *  @copydoc sqlite_statement::bind(int,int)
         */
        void bind(int index, const char *value);

        /**
         *  @copydoc sqlite_statement::bind(int,int)
         */
        void bind(int index, sqlite_blob value);

        /**
         *  Binds the value to the placeholder such as :VVV, @VVV or $VVV (VVV represents an alphanumeric identifier).
         *
         *  @param parameter_name the parameter name equivalent to VVV
         *  @param value          the value to bind to the placeholder
         */
        void bind(const std::string &parameter_name, int value);

        /**
         *  @copydoc sqlite_statement::bind(const std::string &,int)
         */
        void bind(const std::string &parameter_name, sqlite3_int64 value);

        /**
         *  @copydoc sqlite_statement::bind(const std::string &,int)
         */
        void bind(const std::string &parameter_name, double value);

        /**
         *  @copydoc sqlite_statement::bind(const std::string &,int)
         */
        void bind(const std::string &parameter_name, const std::string &value);

        /**
         *  @copydoc sqlite_statement::bind(const std::string &,int)
         */
        void bind(const std::string &parameter_name, const char *value);

        /**
         *  @copydoc sqlite_statement::bind(const std::string &,int)
         */
        void bind(const std::string &parameter_name, sqlite_blob value);

        /**
         *  Removes all binding values.
         */
        void clear_bindings();

    private:
        sqlite_statement(std::shared_ptr<sqlite_holder> db_holder, const std::string &sql);

        sqlite_statement(const sqlite_statement &) = delete;

        sqlite_statement &operator=(const sqlite_statement &) = delete;

        void bind_values_impl(int);

        template<class... ArgType>
        void bind_values_impl(int index, int first_arg, ArgType &&... bind_args);

        template<class... ArgType>
        void bind_values_impl(int index, sqlite3_int64 first_arg, ArgType &&... bind_args);

        template<class... ArgType>
        void bind_values_impl(int index, double first_arg, ArgType &&... bind_args);

        template<class... ArgType>
        void bind_values_impl(int index, std::string first_arg, ArgType &&... bind_args);

        template<class... ArgType>
        void bind_values_impl(int index, const char *first_arg, ArgType &&... bind_args);

        template<class... ArgType>
        void bind_values_impl(int index, sqlite_blob first_arg, ArgType &&... bind_args);

        std::shared_ptr<sqlite_holder> _db_holder;
        std::shared_ptr<sqlite_stmt_holder> _stmt_holder;

        friend class sqlite_database;

        friend class sqlite_query;
    };

    /**
     *  Represents a cursor to fetch data from a row.
     */
    class sqlite_cursor {
    public:
        /**
         *  Gets data from the current row.
         *
         *  @tparam T int, sqlite3_int64, double, std::string, const char *, const void * or fe::sqlite_blob.
         *
         *  @param column_index the zero-based column index.
         */
        template<class T>
        T get(int column_index) const;

        /**
         *  Gets data for the given column name from the current row,
         *  or throws an exception if the column name does not exist.
         *
         *  @tparam T int, sqlite3_int64, double, std::string, const char *, const void * or fe::sqlite_blob.
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
         *  @copydoc sqlite_cursor::get_column_index(const char *)
         */
        int get_column_index(const std::string &column_name) const;

        /**
         *  Returns total number of columns.
         */
        int get_column_count() const;

    private:
        sqlite_cursor(const std::shared_ptr<sqlite_stmt_holder> &stmt_holder);

        std::shared_ptr<sqlite_stmt_holder> _stmt_holder;

        friend class sqlite_iterator;
    };

    /**
     *  Provides an STL-style forward iterator to fetch data from SQLite result set.
     */
    class sqlite_iterator : public std::iterator<std::forward_iterator_tag, sqlite_cursor> {
    public:
        sqlite_iterator(sqlite_iterator &&other) noexcept;

        sqlite_iterator &operator=(sqlite_iterator &&other) noexcept;

        sqlite_iterator &operator++();

        sqlite_cursor &operator*();

        sqlite_cursor *operator->();

        bool operator==(const sqlite_iterator &other) const;

        bool operator!=(const sqlite_iterator &other) const;

    private:
        sqlite_iterator();

        sqlite_iterator(
                const std::shared_ptr<sqlite_holder> &db_holder,
                const std::shared_ptr<sqlite_stmt_holder> &stmt_holder,
                std::uint_fast64_t row_index,
                int state);

        sqlite_iterator(const sqlite_iterator &) = delete;

        sqlite_iterator &operator=(const sqlite_iterator &) = delete;

        std::shared_ptr<sqlite_holder> _db_holder;
        std::shared_ptr<sqlite_stmt_holder> _stmt_holder;
        sqlite_cursor _cursor;
        std::uint_fast64_t _row_index;
        int _state;

        friend class sqlite_query;
    };

    /**
     *  Represents an SQLite result set.
     */
    class sqlite_query {
    public:
        /**
         *  Returns the iterator pointing to the first row.
         *
         *  @attention The iterator got in the past becomes invalid.
         */
        sqlite_iterator begin();

        /**
         *  Returns the iterator to end.
         */
        sqlite_iterator end();

    private:
        sqlite_query(
                const std::shared_ptr<sqlite_holder> &db_holder,
                const std::shared_ptr<sqlite_stmt_holder> &stmt_holder);

        std::shared_ptr<sqlite_holder> _db_holder;
        std::shared_ptr<sqlite_stmt_holder> _stmt_holder;

        friend class sqlite_statement;

        friend class sqlite_database;
    };

    /**
     *  Describes the SQLite transaction modes.
     */
    enum class sqlite_transaction_mode {
        deferred,
        immediate,
        exclusive,
    };

    /**
     *  Represents a transaction using the RAII idiom.
     */
    class sqlite_transaction {
    public:
        /**
         *  Destructor.
         *  Rollbacks this transaction if not committed, or does nothing if committed.
         */
        ~sqlite_transaction() noexcept;

        /**
         *  Move constructor.
         */
        sqlite_transaction(sqlite_transaction &&other) noexcept;

        /**
         *  Move assignment operator.
         */
        sqlite_transaction &operator=(sqlite_transaction &&other) noexcept;

        /**
         *  Commits this transaction.
         */
        void commit();

    private:
        /**
         *  Constructor.
         *  Begin a transaction.
         */
        sqlite_transaction(sqlite_database *db, sqlite_transaction_mode mode);

        sqlite_transaction(const sqlite_transaction &) = delete;

        sqlite_transaction &operator=(const sqlite_transaction &) = delete;

        sqlite_database *_db;
        bool _in_transaction;

        friend class sqlite_database;
    };

    /**
     *  Used for receiving notifications when the user version of the SQLite database has changed and so on.
     */
    struct sqlite_listener {
        /**
         *  Called when the SQLite database needs to be upgraded.
         */
        std::function<void(sqlite_database *db, int old_version, int new_version)> on_upgrade;

        /**
         *  Called when the SQLite database needs to be downgraded.
         */
        std::function<void(sqlite_database *db, int old_version, int new_version)> on_downgrade;
    };

    /**
     *  Represents an SQLite database.
     */
    class sqlite_database {
    public:
        /**
         *  Constructor to create an in-memory database.
         */
        sqlite_database();

        /**
         *  Constructor.
         *
         *  @param path the path of the SQLite database file to open and/or create.
         */
        sqlite_database(const std::string &path);

        /**
         *  Move constructor.
         */
        sqlite_database(sqlite_database &&other) noexcept;

        /**
         *  Move assignment operator.
         */
        sqlite_database &operator=(sqlite_database &&other) noexcept;

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
         *  Executes the given precompiled SQL statement.
         */
        void exec(sqlite_statement &statement);

        /**
         *  Executes the given precompiled SQL statement.
         *  Removes all bindings of the given statement
         *  and rebind the arguments to the placeholders before the execution.
         */
        template<class... ArgType>
        void exec(sqlite_statement &statement, ArgType &&... bind_args);

        /**
         *  Runs the given SQL statement that returns data such as SELECT.
         */
        sqlite_query query(const std::string &sql);

        /**
         *  Runs the given SQL statement that returns data such as SELECT.
         *
         *  @param sql       the single SQL statement.
         *  @param bind_args the values to bind to the placeholders such as ?
         */
        template<class... ArgType>
        sqlite_query query(const std::string &sql, ArgType &&... bind_args);

        /**
         *  Creates a precompiled SQL statement.
         */
        sqlite_statement prepare_statement(const std::string &sql);

        /**
         *  Begins a transaction.
         *
         *  @param mode transaction mode
         */
        void begin_transaction(sqlite_transaction_mode mode = sqlite_transaction_mode::deferred);

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
        sqlite_transaction create_transaction(sqlite_transaction_mode mode = sqlite_transaction_mode::deferred);

        /**
         *  Gets the fe::sqlite_listener.
         */
        const sqlite_listener &get_listener() const;

        /**
         *  Sets the fe::sqlite_listener.
         */
        void set_listener(const sqlite_listener &listener);

        /**
         *  Sets the fe::sqlite_listener.
         */
        void set_listener(sqlite_listener &&listener);

        /**
         *  Gets the user version of the database, or 0 that is a default value.
         */
        int get_version();

        /**
         *  Updates the user version of the database in a transaction.
         *
         *  @param version the new version that must be > 0.
         *  @param mode    the transaction mode.
         */
        void update_version(int version, sqlite_transaction_mode mode = sqlite_transaction_mode::deferred);

        /**
         *  Returns the file path of the database, or ":memory:" if the in-memory database.
         */
        const std::string &get_path() const;

    private:
        sqlite_database(const sqlite_database &) = delete;

        sqlite_database &operator=(const sqlite_database &) = delete;

        std::string _path;
        std::shared_ptr<sqlite_holder> _db_holder;
        sqlite_listener _listener;

        friend class sqlite_statement;

        friend class sqlite_iterator;
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

    sqlite_holder::~sqlite_holder() noexcept {
        if (_db) {
            sqlite3_close_v2(_db);
        }
    }

    void sqlite_holder::close() {
        if (!_db) {
            return;
        }

        auto rc = sqlite3_close_v2(_db);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to close database", rc);
        }

        _db = nullptr;
    }

    sqlite3 *sqlite_holder::handle() const {
        if (_db == nullptr) {
            throw std::logic_error("database is closed");
        }
        return _db;
    }

#pragma mark ## sqlite_stmt_holder ##

    inline sqlite_stmt_holder::sqlite_stmt_holder(sqlite3_stmt *stmt) noexcept : _stmt(stmt) {
    }

    sqlite_stmt_holder::~sqlite_stmt_holder() noexcept {
        if (_stmt) {
            sqlite3_finalize(_stmt);
        }
    }

    void sqlite_stmt_holder::finalize() {
        auto rc = sqlite3_finalize(_stmt);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to finalize statement", rc);
        }
        _stmt = nullptr;
    }

    sqlite3_stmt *sqlite_stmt_holder::handle() const {
        if (_stmt == nullptr) {
            throw std::logic_error("statement is finalized");
        }
        return _stmt;
    }

#pragma mark ## sqlite_statement ##

    inline sqlite_statement::sqlite_statement(sqlite_statement &&other) noexcept
            : _db_holder(std::move(other._db_holder)), _stmt_holder(std::move(other._stmt_holder)) {
    }

    inline sqlite_statement &sqlite_statement::operator=(sqlite_statement &&other) noexcept {
        _db_holder = std::move(other._db_holder);
        _stmt_holder = std::move(other._stmt_holder);

        return *this;
    }

    inline void sqlite_statement::finalize() {
        _stmt_holder->finalize();
    }

    inline void sqlite_statement::exec() {
        query().begin();
    }

    inline sqlite_query sqlite_statement::query() {
        return sqlite_query(_db_holder, _stmt_holder);
    }

    template<class... ArgType>
    void sqlite_statement::bind_values(ArgType &&... bind_args) {
        bind_values_impl(1, std::forward<ArgType>(bind_args)...);
    }

    inline void sqlite_statement::bind(int index, int value) {
        auto rc = sqlite3_bind_int(_stmt_holder->handle(), index, value);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to bind int", rc);
        }
    }

    inline void sqlite_statement::bind(int index, sqlite3_int64 value) {
        auto rc = sqlite3_bind_int64(_stmt_holder->handle(), index, value);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to bind int64", rc);
        }
    }

    inline void sqlite_statement::bind(int index, double value) {
        auto rc = sqlite3_bind_double(_stmt_holder->handle(), index, value);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to bind double", rc);
        }
    }

    inline void sqlite_statement::bind(int index, const std::string &value) {
        auto rc = sqlite3_bind_text(_stmt_holder->handle(), index, value.c_str(), static_cast<int>(value.length()), SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to bind text", rc);
        }
    }

    inline void sqlite_statement::bind(int index, const char *value) {
        auto rc = sqlite3_bind_text(_stmt_holder->handle(), index, value, static_cast<int>(std::strlen(value)), SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to bind text", rc);
        }
    }

    inline void sqlite_statement::bind(int index, sqlite_blob value) {
        auto rc = sqlite3_bind_blob(_stmt_holder->handle(), index, value.data, value.size, SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to bind blob", rc);
        }
    }

    inline void sqlite_statement::bind(const std::string &parameter_name, int value) {
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

    inline void sqlite_statement::bind(const std::string &parameter_name, sqlite3_int64 value) {
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

    inline void sqlite_statement::bind(const std::string &parameter_name, double value) {
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

    inline void sqlite_statement::bind(const std::string &parameter_name, const std::string &value) {
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

    inline void sqlite_statement::bind(const std::string &parameter_name, const char *value) {
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

    inline void sqlite_statement::bind(const std::string &parameter_name, sqlite_blob value) {
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

    inline void sqlite_statement::clear_bindings() {
        auto rc = sqlite3_clear_bindings(_stmt_holder->handle());
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to clear bindings", rc);
        }
    }

    inline sqlite_statement::sqlite_statement(std::shared_ptr<sqlite_holder> db_holder, const std::string &sql)
            : _db_holder(std::move(db_holder)) {
        sqlite3_stmt *stmt;
        auto rc = sqlite3_prepare_v2(_db_holder->handle(), sql.c_str(), static_cast<int>(sql.length()), &stmt, nullptr);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to prepare statement, SQL = \"" + sql + "\"", rc);
        }
        _stmt_holder = std::make_shared<sqlite_stmt_holder>(stmt);
    }

    void sqlite_statement::bind_values_impl(int) {
    }

    template<class... ArgType>
    void sqlite_statement::bind_values_impl(int index, int first_arg, ArgType &&... bind_args) {
        auto rc = sqlite3_bind_int(_stmt_holder->handle(), index, first_arg);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to bind int", rc);
        }
        bind_values_impl(index + 1, std::forward<ArgType>(bind_args)...);
    }

    template<class... ArgType>
    void sqlite_statement::bind_values_impl(int index, sqlite3_int64 first_arg, ArgType &&... bind_args) {
        auto rc = sqlite3_bind_int64(_stmt_holder->handle(), index, first_arg);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to bind int64", rc);
        }
        bind_values_impl(index + 1, std::forward<ArgType>(bind_args)...);
    }

    template<class... ArgType>
    void sqlite_statement::bind_values_impl(int index, double first_arg, ArgType &&... bind_args) {
        auto rc = sqlite3_bind_double(_stmt_holder->handle(), index, first_arg);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to bind double", rc);
        }
        bind_values_impl(index + 1, std::forward<ArgType>(bind_args)...);
    }

    template<class... ArgType>
    void sqlite_statement::bind_values_impl(int index, std::string first_arg, ArgType &&... bind_args) {
        auto rc = sqlite3_bind_text(_stmt_holder->handle(), index, first_arg.c_str(), static_cast<int>(first_arg.length()),
                SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to bind text", rc);
        }
        bind_values_impl(index + 1, std::forward<ArgType>(bind_args)...);
    }

    template<class... ArgType>
    void sqlite_statement::bind_values_impl(int index, const char *first_arg, ArgType &&... bind_args) {
        auto rc = sqlite3_bind_text(_stmt_holder->handle(), index, first_arg, static_cast<int>(std::strlen(first_arg)),
                SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to bind text", rc);
        }
        bind_values_impl(index + 1, std::forward<ArgType>(bind_args)...);
    }

    template<class... ArgType>
    void sqlite_statement::bind_values_impl(int index, sqlite_blob first_arg, ArgType &&... bind_args) {
        auto rc = sqlite3_bind_blob(_stmt_holder->handle(), index, first_arg.data, first_arg.size, SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to bind blob", rc);
        }
        bind_values_impl(index + 1, std::forward<ArgType>(bind_args)...);
    }

#pragma mark ## sqlite_cursor ##

    template<>
    inline int sqlite_cursor::get(int column_index) const {
        return sqlite3_column_int(_stmt_holder->handle(), column_index);
    }

    template<>
    inline sqlite3_int64 sqlite_cursor::get(int column_index) const {
        return sqlite3_column_int64(_stmt_holder->handle(), column_index);
    }

    template<>
    inline double sqlite_cursor::get(int column_index) const {
        return sqlite3_column_double(_stmt_holder->handle(), column_index);
    }

    template<>
    inline const unsigned char *sqlite_cursor::get(int column_index) const {
        return sqlite3_column_text(_stmt_holder->handle(), column_index);
    }

    template<>
    inline const char *sqlite_cursor::get(int column_index) const {
        return reinterpret_cast<const char *>(sqlite3_column_text(_stmt_holder->handle(), column_index));
    }

    template<>
    inline std::string sqlite_cursor::get(int column_index) const {
        return reinterpret_cast<const char *>(sqlite3_column_text(_stmt_holder->handle(), column_index));
    }

    template<>
    inline const void *sqlite_cursor::get(int column_index) const {
        return sqlite3_column_blob(_stmt_holder->handle(), column_index);
    }

    template<>
    inline sqlite_blob sqlite_cursor::get(int column_index) const {
        sqlite_blob blob;
        blob.data = sqlite3_column_text(_stmt_holder->handle(), column_index);
        blob.size = static_cast<int>(std::strlen(reinterpret_cast<const char *>(blob.data)) - 1);
        return blob;
    }

    template<class T>
    T sqlite_cursor::get(const std::string &column_name) const {
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

    inline std::string sqlite_cursor::get_column_name(int column_index) const {
        return sqlite3_column_name(_stmt_holder->handle(), column_index);
    }

    inline int sqlite_cursor::get_column_index(const char *column_name) const {
        for (int i = 0, n = sqlite3_column_count(_stmt_holder->handle()); i < n; ++i) {
            if (0 == std::strcmp(sqlite3_column_name(_stmt_holder->handle(), i), column_name)) {
                return i;
            }
        }
        return -1;
    }

    inline int sqlite_cursor::get_column_index(const std::string &column_name) const {
        return get_column_index(column_name.c_str());
    }

    inline int sqlite_cursor::get_column_count() const {
        return sqlite3_column_count(_stmt_holder->handle());
    }

    inline sqlite_cursor::sqlite_cursor(const std::shared_ptr<sqlite_stmt_holder> &stmt_holder)
            : _stmt_holder(stmt_holder) {
    }

#pragma mark ## sqlite_iterator ##

    inline sqlite_iterator::sqlite_iterator(sqlite_iterator &&other) noexcept : _db_holder(std::move(other._db_holder)),
                                                                                _stmt_holder(std::move(other._stmt_holder)),
                                                                                _cursor(std::move(other._cursor)),
                                                                                _row_index(other._row_index),
                                                                                _state(other._state) {
    }

    inline sqlite_iterator &sqlite_iterator::operator=(sqlite_iterator &&other) noexcept {
        _db_holder = std::move(other._db_holder);
        _stmt_holder = std::move(other._stmt_holder);
        _cursor = std::move(other._cursor);
        _row_index = other._row_index;
        _state = other._state;

        return *this;
    }

    inline sqlite_iterator &sqlite_iterator::operator++() {
        _state = sqlite3_step(_stmt_holder->handle());

        if (_state != SQLITE_ROW && _state != SQLITE_DONE) {
            throw sqlite_error("Failed to step statement", _state);
        }

        _row_index++;
        return *this;
    }

    inline sqlite_cursor &sqlite_iterator::operator*() {
        return _cursor;
    }

    inline sqlite_cursor *sqlite_iterator::operator->() {
        return &_cursor;
    }

    inline bool sqlite_iterator::operator==(const sqlite_iterator &other) const {
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

    inline bool sqlite_iterator::operator!=(const sqlite_iterator &other) const {
        return !operator==(other);
    }

    inline sqlite_iterator::sqlite_iterator()
            : _db_holder(), _stmt_holder(), _cursor(nullptr), _row_index(0), _state(-1) {
    }

    inline sqlite_iterator::sqlite_iterator(
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

#pragma mark ## sqlite_query ##

    inline sqlite_iterator sqlite_query::begin() {
        auto rc = sqlite3_reset(_stmt_holder->handle());
        if (rc != SQLITE_OK) {
            throw sqlite_error("Failed to reset statement", rc);
        }

        rc = sqlite3_step(_stmt_holder->handle());
        if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
            throw sqlite_error("Failed to step statement", rc);
        }

        return sqlite_iterator(_db_holder, _stmt_holder, 0, rc);
    }

    inline sqlite_iterator sqlite_query::end() {
        return sqlite_iterator();
    }

    inline sqlite_query::sqlite_query(
            const std::shared_ptr<sqlite_holder> &db_holder,
            const std::shared_ptr<sqlite_stmt_holder> &stmt_holder)
            : _db_holder(db_holder), _stmt_holder(stmt_holder) {
    }

#pragma mark ## sqlite_transaction ##

    inline sqlite_transaction::~sqlite_transaction() noexcept {
        if (_in_transaction && _db) {
            try {
                _db->rollback_transaction();
            } catch (...) {
                // ignore
            }
        }
    }

    inline sqlite_transaction::sqlite_transaction(sqlite_transaction &&other) noexcept
            : _db(other._db),
              _in_transaction(other._in_transaction) {
        other._db = nullptr;
        other._in_transaction = false;
    }

    inline sqlite_transaction &sqlite_transaction::operator=(sqlite_transaction &&other) noexcept {
        _db = other._db;
        _in_transaction = other._in_transaction;

        other._db = nullptr;
        other._in_transaction = false;

        return *this;
    }

    inline void sqlite_transaction::commit() {
        _db->commit_transaction();
        _in_transaction = false;
    }

    inline sqlite_transaction::sqlite_transaction(sqlite_database *db, sqlite_transaction_mode mode) : _db(db) {
        _db->begin_transaction(mode);
        _in_transaction = true;
    }

#pragma mark ## sqlite_database ##

    inline sqlite_database::sqlite_database() : _path(":memory:") {
    }

    inline sqlite_database::sqlite_database(const std::string &path) : _path(path) {
    }

    inline sqlite_database::sqlite_database(sqlite_database &&other) noexcept : _path(std::move(other._path)),
                                                                                _db_holder(std::move(other._db_holder)) {
    }

    inline sqlite_database &sqlite_database::operator=(sqlite_database &&other) noexcept {
        _path = std::move(other._path);
        _db_holder = std::move(other._db_holder);
        return *this;
    }

    inline void sqlite_database::open() {
        sqlite3 *db;
        auto rc = sqlite3_open(_path.c_str(), &db);
        if (rc != SQLITE_OK) {
            sqlite3_close(db);
            throw sqlite_error("Failed to open database", rc);
        }
        _db_holder = std::make_shared<sqlite_holder>(db);
    }

#ifdef SQLITE_HAS_CODEC

    inline void sqlite_database::open(const std::string &passphrase) {
        open();
        sqlite3_key(_db_holder->handle(), passphrase.c_str(), static_cast<int>(passphrase.length()));
    }

#endif

    inline void sqlite_database::close() {
        _db_holder->close();
    }

    inline void sqlite_database::exec_sql(const std::string &sql) {
        sqlite_statement statement(_db_holder, sql);
        statement.exec();
        statement.finalize();
    }

    template<class... ArgType>
    void sqlite_database::exec_sql(const std::string &sql, ArgType &&... bind_args) {
        sqlite_statement statement(_db_holder, sql);
        statement.bind_values(std::forward<ArgType>(bind_args)...);
        statement.exec();
        statement.finalize();
    }

    inline void sqlite_database::exec(sqlite_statement &statement) {
        statement.exec();
    }

    template<class... ArgType>
    void sqlite_database::exec(sqlite_statement &statement, ArgType &&... bind_args) {
        statement.clear_bindings();
        statement.bind_values(std::forward<ArgType>(bind_args)...);
        statement.exec();
    }

    inline sqlite_query sqlite_database::query(const std::string &sql) {
        sqlite_statement statement(_db_holder, sql);
        return statement.query();
    }

    template<class... ArgType>
    sqlite_query sqlite_database::query(const std::string &sql, ArgType &&... bind_args) {
        sqlite_statement statement(_db_holder, sql);
        statement.bind_values(std::forward<ArgType>(bind_args)...);
        return statement.query();
    }

    inline sqlite_statement sqlite_database::prepare_statement(const std::string &sql) {
        return sqlite_statement(_db_holder, sql);
    }

    inline void sqlite_database::begin_transaction(sqlite_transaction_mode mode) {
        const char *sql;
        switch (mode) {
            case sqlite_transaction_mode::deferred:
                sql = "BEGIN DEFERRED;";
                break;

            case sqlite_transaction_mode::immediate:
                sql = "BEGIN IMMEDIATE;";
                break;

            case sqlite_transaction_mode::exclusive:
                sql = "BEGIN EXCLUSIVE;";
                break;
        }

        exec_sql(sql);
    }

    inline void sqlite_database::commit_transaction() {
        exec_sql("COMMIT;");
    }

    inline void sqlite_database::rollback_transaction() {
        exec_sql("ROLLBACK;");
    }

    inline bool sqlite_database::is_open() const {
        return _db_holder.get() == nullptr;
    }

    inline sqlite_transaction sqlite_database::create_transaction(sqlite_transaction_mode mode) {
        return sqlite_transaction(this, mode);
    }

    inline const sqlite_listener &sqlite_database::get_listener() const {
        return _listener;
    }

    void sqlite_database::set_listener(const sqlite_listener &listener) {
        _listener = listener;
    }


    void sqlite_database::set_listener(sqlite_listener &&listener) {
        _listener = std::move(listener);
    }

    inline int sqlite_database::get_version() {
        auto &&results = query("PRAGMA user_version;");

        int result = 0;
        for (auto &&cursor : results) {
            result = cursor.get<int>(0);
        }

        return result;
    }

    inline void sqlite_database::update_version(int version, sqlite_transaction_mode mode) {
        if (version < 1) {
            throw std::logic_error("Invalid version, must be > 0");
        }

        auto old_version = get_version();
        if (old_version == version) {
            return;
        }

        auto transaction = create_transaction(mode);

        if (old_version < version) {
            if (_listener.on_upgrade) {
                _listener.on_upgrade(this, old_version, version);
            }
        } else if (old_version > version) {
            if (_listener.on_downgrade) {
                _listener.on_downgrade(this, old_version, version);
            }
        }

        std::stringstream ss;
        ss << "PRAGMA user_version = " << version << ";";
        exec_sql(ss.str());

        transaction.commit();
    }

    inline const std::string &sqlite_database::get_path() const {
        return _path;
    }
}
