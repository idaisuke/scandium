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
#include <stdexcept>
#include <sstream>
#include <string>
#include <thread>
#include <utility>

#include "sqlite3.h"

//TODO: sqlite_iteratorのend()との比較を再考
//TODO: blob64
//TODO: exec_sqlとqueryの統合
//TODO: sqlite_database*, sqlite_statement* -> sqlite3_db**, sqlite3_stmt
//TODO: sqlite_database -> database
//TODO: sqlite_exception -> sqlite3_error & std::runtime_error
//TODO: statement.bind_values -> static method
namespace scandium {

    class sqlite_database;

    class sqlite_iterator;

    class sqlite_result_set;

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
    class sqlite_exception : public std::runtime_error {
    public:
        sqlite_exception(const char *what);

        sqlite_exception(const char *what, int rc);

        sqlite_exception(const std::string &what);

        sqlite_exception(const std::string &what, int rc);

        const int rc;
    };

    /**
     *  Represents a precompiled SQL statement.
     */
    class sqlite_statement {
    public:
        /**
         *  Destructor.
         *  Finalizes this statement if not finalized.
         */
        ~sqlite_statement() noexcept;

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
        sqlite_result_set query();

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
        sqlite_statement(sqlite3 *db, const std::string &sql);

        sqlite_statement(const sqlite_statement &) = delete;

        sqlite_statement &operator=(const sqlite_statement &) = delete;

        void exec();

        void reset();

        void bind_values_internal(int);

        template<class... ArgType>
        void bind_values_internal(int index, int first_arg, ArgType &&... bind_args);

        template<class... ArgType>
        void bind_values_internal(int index, sqlite3_int64 first_arg, ArgType &&... bind_args);

        template<class... ArgType>
        void bind_values_internal(int index, double first_arg, ArgType &&... bind_args);

        template<class... ArgType>
        void bind_values_internal(int index, std::string first_arg, ArgType &&... bind_args);

        template<class... ArgType>
        void bind_values_internal(int index, const char *first_arg, ArgType &&... bind_args);

        template<class... ArgType>
        void bind_values_internal(int index, sqlite_blob first_arg, ArgType &&... bind_args);

        sqlite3 *_db;
        sqlite3_stmt *_stmt;

        friend class sqlite_database;

        friend class sqlite_result_set;
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
        sqlite_cursor(sqlite3_stmt *stmt);

        sqlite3_stmt *_stmt;

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

        sqlite_iterator(sqlite3 *db, sqlite3_stmt *stmt, std::uint_fast64_t row_index, int state);

        sqlite_iterator(const sqlite_iterator &) = delete;

        sqlite_iterator &operator=(const sqlite_iterator &) = delete;

        sqlite3 *_db;
        sqlite3_stmt *_stmt;
        sqlite_cursor _cursor;
        std::uint_fast64_t _row_index;
        int _state;

        friend class sqlite_result_set;
    };

    /**
     *  Represents an SQLite result set.
     */
    class sqlite_result_set {
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
        sqlite_result_set(sqlite3 *db, sqlite3_stmt *stmt);

        sqlite3 *_db;
        sqlite3_stmt *_stmt;

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
         *  Destructor.
         *  Closes this database if not closed.
         */
        ~sqlite_database() noexcept;

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
        sqlite_result_set query(const std::string &sql);

        /**
         *  Runs the given SQL statement that returns data such as SELECT.
         *
         *  @param sql       the single SQL statement.
         *  @param bind_args the values to bind to the placeholders such as ?
         */
        template<class... ArgType>
        sqlite_result_set query(const std::string &sql, ArgType &&... bind_args);

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

        int open_internal() noexcept;

        int close_internal() noexcept;

        std::string _path;
        sqlite3 *_db = nullptr;
        sqlite_listener _listener;

        friend class sqlite_statement;

        friend class sqlite_iterator;
    };

#pragma mark ## sqlite_exception ##

    inline sqlite_exception::sqlite_exception(const char *what) : std::runtime_error(what), rc(-1) {
    }

    inline sqlite_exception::sqlite_exception(const char *what, int rc) : std::runtime_error(what), rc(rc) {
    }

    inline sqlite_exception::sqlite_exception(const std::string &what) : std::runtime_error(what), rc(-1) {
    }

    inline sqlite_exception::sqlite_exception(const std::string &what, int rc) : std::runtime_error(what), rc(rc) {
    }

#pragma mark ## sqlite_statement ##

    inline sqlite_statement::~sqlite_statement() noexcept {
        sqlite3_finalize(_stmt);
    }

    inline sqlite_statement::sqlite_statement(sqlite_statement &&other) noexcept : _db(other._db), _stmt(other._stmt) {
        other._db = nullptr;
        other._stmt = nullptr;
    }

    inline sqlite_statement &sqlite_statement::operator=(sqlite_statement &&other) noexcept {
        _db = other._db;
        _stmt = other._stmt;

        other._db = nullptr;
        other._stmt = nullptr;

        return *this;
    }

    inline void sqlite_statement::finalize() {
        auto rc = sqlite3_finalize(_stmt);
        if (rc != SQLITE_OK) {
            throw sqlite_exception("Failed to finalize statement", rc);
        }
        _stmt = nullptr;
    }

    inline sqlite_result_set sqlite_statement::query() {
        return sqlite_result_set(_db, _stmt);
    }

    template<class... ArgType>
    void sqlite_statement::bind_values(ArgType &&... bind_args) {
        bind_values_internal(1, std::forward<ArgType>(bind_args)...);
    }

    inline void sqlite_statement::bind(int index, int value) {
        auto rc = sqlite3_bind_int(_stmt, index, value);
        if (rc != SQLITE_OK) {
            throw sqlite_exception("Failed to bind int", rc);
        }
    }

    inline void sqlite_statement::bind(int index, sqlite3_int64 value) {
        auto rc = sqlite3_bind_int64(_stmt, index, value);
        if (rc != SQLITE_OK) {
            throw sqlite_exception("Failed to bind int64", rc);
        }
    }

    inline void sqlite_statement::bind(int index, double value) {
        auto rc = sqlite3_bind_double(_stmt, index, value);
        if (rc != SQLITE_OK) {
            throw sqlite_exception("Failed to bind double", rc);
        }
    }

    inline void sqlite_statement::bind(int index, const std::string &value) {
        auto rc = sqlite3_bind_text(_stmt, index, value.c_str(), static_cast<int>(value.length()), SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) {
            throw sqlite_exception("Failed to bind text", rc);
        }
    }

    inline void sqlite_statement::bind(int index, const char *value) {
        auto rc = sqlite3_bind_text(_stmt, index, value, static_cast<int>(std::strlen(value)), SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) {
            throw sqlite_exception("Failed to bind text", rc);
        }
    }

    inline void sqlite_statement::bind(int index, sqlite_blob value) {
        auto rc = sqlite3_bind_blob(_stmt, index, value.data, value.size, SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) {
            throw sqlite_exception("Failed to bind blob", rc);
        }
    }

    inline void sqlite_statement::bind(const std::string &parameter_name, int value) {
        auto index = sqlite3_bind_parameter_index(_stmt, parameter_name.c_str());
        if (index == 0) {
            std::string what;
            what.append("No matching parameter named '");
            what.append(parameter_name);
            what.append("' is found");
            throw sqlite_exception(what);
        }

        bind(index, value);
    }

    inline void sqlite_statement::bind(const std::string &parameter_name, sqlite3_int64 value) {
        auto index = sqlite3_bind_parameter_index(_stmt, parameter_name.c_str());
        if (index == 0) {
            std::string what;
            what.append("No matching parameter named '");
            what.append(parameter_name);
            what.append("' is found");
            throw sqlite_exception(what);
        }

        bind(index, value);
    }

    inline void sqlite_statement::bind(const std::string &parameter_name, double value) {
        auto index = sqlite3_bind_parameter_index(_stmt, parameter_name.c_str());
        if (index == 0) {
            std::string what;
            what.append("No matching parameter named '");
            what.append(parameter_name);
            what.append("' is found");
            throw sqlite_exception(what);
        }

        bind(index, value);
    }

    inline void sqlite_statement::bind(const std::string &parameter_name, const std::string &value) {
        auto index = sqlite3_bind_parameter_index(_stmt, parameter_name.c_str());
        if (index == 0) {
            std::string what;
            what.append("No matching parameter named '");
            what.append(parameter_name);
            what.append("' is found");
            throw sqlite_exception(what);
        }

        bind(index, value);
    }

    inline void sqlite_statement::bind(const std::string &parameter_name, const char *value) {
        auto index = sqlite3_bind_parameter_index(_stmt, parameter_name.c_str());
        if (index == 0) {
            std::string what;
            what.append("No matching parameter named '");
            what.append(parameter_name);
            what.append("' is found");
            throw sqlite_exception(what);
        }

        bind(index, value);
    }

    inline void sqlite_statement::bind(const std::string &parameter_name, sqlite_blob value) {
        auto index = sqlite3_bind_parameter_index(_stmt, parameter_name.c_str());
        if (index == 0) {
            std::string what;
            what.append("No matching parameter named '");
            what.append(parameter_name);
            what.append("' is found");
            throw sqlite_exception(what);
        }

        bind(index, std::move(value));
    }

    inline void sqlite_statement::clear_bindings() {
        auto rc = sqlite3_clear_bindings(_stmt);
        if (rc != SQLITE_OK) {
            throw sqlite_exception("Failed to clear bindings", rc);
        }
    }

    inline sqlite_statement::sqlite_statement(sqlite3 *db, const std::string &sql) : _db(db) {
        auto rc = sqlite3_prepare_v2(_db, sql.c_str(), static_cast<int>(sql.length()), &_stmt, nullptr);
        if (rc != SQLITE_OK) {
            throw sqlite_exception("Failed to prepare statement, SQL = \"" + sql + "\"", rc);
        }
    }

    inline void sqlite_statement::exec() {
        auto rc = sqlite3_step(_stmt);

        if (rc == SQLITE_ROW) {
            throw sqlite_exception("This method must not be an SQL statement that returns data");
        }

        if (rc != SQLITE_DONE) {
            throw sqlite_exception("Failed to step statement", rc);
        }
    }

    inline void sqlite_statement::reset() {
        auto rc = sqlite3_reset(_stmt);
        if (rc != SQLITE_OK) {
            throw sqlite_exception("Failed to reset statement", rc);
        }
    }

    void sqlite_statement::bind_values_internal(int) {
    }

    template<class... ArgType>
    void sqlite_statement::bind_values_internal(int index, int first_arg, ArgType &&... bind_args) {
        auto rc = sqlite3_bind_int(_stmt, index, first_arg);
        if (rc != SQLITE_OK) {
            throw sqlite_exception("Failed to bind int", rc);
        }
        bind_values_internal(index + 1, std::forward<ArgType>(bind_args)...);
    }

    template<class... ArgType>
    void sqlite_statement::bind_values_internal(int index, sqlite3_int64 first_arg, ArgType &&... bind_args) {
        auto rc = sqlite3_bind_int64(_stmt, index, first_arg);
        if (rc != SQLITE_OK) {
            throw sqlite_exception("Failed to bind int64", rc);
        }
        bind_values_internal(index + 1, std::forward<ArgType>(bind_args)...);
    }

    template<class... ArgType>
    void sqlite_statement::bind_values_internal(int index, double first_arg, ArgType &&... bind_args) {
        auto rc = sqlite3_bind_double(_stmt, index, first_arg);
        if (rc != SQLITE_OK) {
            throw sqlite_exception("Failed to bind double", rc);
        }
        bind_values_internal(index + 1, std::forward<ArgType>(bind_args)...);
    }

    template<class... ArgType>
    void sqlite_statement::bind_values_internal(int index, std::string first_arg, ArgType &&... bind_args) {
        auto rc = sqlite3_bind_text(_stmt, index, first_arg.c_str(), static_cast<int>(first_arg.length()),
                SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) {
            throw sqlite_exception("Failed to bind text", rc);
        }
        bind_values_internal(index + 1, std::forward<ArgType>(bind_args)...);
    }

    template<class... ArgType>
    void sqlite_statement::bind_values_internal(int index, const char *first_arg, ArgType &&... bind_args) {
        auto rc = sqlite3_bind_text(_stmt, index, first_arg, static_cast<int>(std::strlen(first_arg)),
                SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) {
            throw sqlite_exception("Failed to bind text", rc);
        }
        bind_values_internal(index + 1, std::forward<ArgType>(bind_args)...);
    }

    template<class... ArgType>
    void sqlite_statement::bind_values_internal(int index, sqlite_blob first_arg, ArgType &&... bind_args) {
        auto rc = sqlite3_bind_blob(_stmt, index, first_arg.data, first_arg.size, SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) {
            throw sqlite_exception("Failed to bind blob", rc);
        }
        bind_values_internal(index + 1, std::forward<ArgType>(bind_args)...);
    }

#pragma mark ## sqlite_cursor ##

    template<>
    inline int sqlite_cursor::get(int column_index) const {
        return sqlite3_column_int(_stmt, column_index);
    }

    template<>
    inline sqlite3_int64 sqlite_cursor::get(int column_index) const {
        return sqlite3_column_int64(_stmt, column_index);
    }

    template<>
    inline double sqlite_cursor::get(int column_index) const {
        return sqlite3_column_double(_stmt, column_index);
    }

    template<>
    inline const unsigned char *sqlite_cursor::get(int column_index) const {
        return sqlite3_column_text(_stmt, column_index);
    }

    template<>
    inline const char *sqlite_cursor::get(int column_index) const {
        return reinterpret_cast<const char *>(sqlite3_column_text(_stmt, column_index));
    }

    template<>
    inline std::string sqlite_cursor::get(int column_index) const {
        return reinterpret_cast<const char *>(sqlite3_column_text(_stmt, column_index));
    }

    template<>
    inline const void *sqlite_cursor::get(int column_index) const {
        return sqlite3_column_blob(_stmt, column_index);
    }

    template<>
    inline sqlite_blob sqlite_cursor::get(int column_index) const {
        sqlite_blob blob;
        blob.data = sqlite3_column_text(_stmt, column_index);
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
            throw sqlite_exception(what);
        }
        return get<T>(index);
    }

    inline std::string sqlite_cursor::get_column_name(int column_index) const {
        return sqlite3_column_name(_stmt, column_index);
    }

    inline int sqlite_cursor::get_column_index(const char *column_name) const {
        for (int i = 0, n = sqlite3_column_count(_stmt); i < n; ++i) {
            if (0 == std::strcmp(sqlite3_column_name(_stmt, i), column_name)) {
                return i;
            }
        }
        return -1;
    }

    inline int sqlite_cursor::get_column_index(const std::string &column_name) const {
        return get_column_index(column_name.c_str());
    }

    inline int sqlite_cursor::get_column_count() const {
        return sqlite3_column_count(_stmt);
    }

    inline sqlite_cursor::sqlite_cursor(sqlite3_stmt *stmt) : _stmt(stmt) {
    }

#pragma mark ## sqlite_iterator ##

    inline sqlite_iterator::sqlite_iterator(sqlite_iterator &&other) noexcept : _db(other._db),
                                                                                _stmt(other._stmt),
                                                                                _cursor(other._stmt),
                                                                                _row_index(other._row_index),
                                                                                _state(other._state) {
        other._db = nullptr;
        other._stmt = nullptr;
        other._row_index = -1;
        other._state = -1;
    }

    inline sqlite_iterator &sqlite_iterator::operator=(sqlite_iterator &&other) noexcept {
        _db = other._db;
        _stmt = other._stmt;
        _row_index = other._row_index;
        _state = other._state;

        other._db = nullptr;
        other._stmt = nullptr;
        other._row_index = -1;
        other._state = -1;
        return *this;
    }

    inline sqlite_iterator &sqlite_iterator::operator++() {
        _state = sqlite3_step(_stmt);

        if (_state != SQLITE_ROW && _state != SQLITE_DONE) {
            throw sqlite_exception("Failed to step statement", _state);
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

        return _db == other._db
                && _stmt == other._stmt
                && _row_index == other._row_index
                && _state == other._state;
    }

    inline bool sqlite_iterator::operator!=(const sqlite_iterator &other) const {
        return !operator==(other);
    }

    inline sqlite_iterator::sqlite_iterator()
            : _db(nullptr), _stmt(nullptr), _cursor(nullptr), _row_index(0), _state(-1) {
    }

    inline sqlite_iterator::sqlite_iterator(sqlite3 *db, sqlite3_stmt *stmt, std::uint_fast64_t row_index, int state)
            : _db(db), _stmt(stmt), _cursor(stmt), _row_index(row_index), _state(state) {
    }

#pragma mark ## sqlite_result_set ##

    inline sqlite_iterator sqlite_result_set::begin() {
        auto rc = sqlite3_reset(_stmt);
        if (rc != SQLITE_OK) {
            throw sqlite_exception("Failed to reset statement", rc);
        }

        rc = sqlite3_step(_stmt);
        if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
            throw sqlite_exception("Failed to step statement", rc);
        }

        return sqlite_iterator(_db, _stmt, 0, rc);
    }

    inline sqlite_iterator sqlite_result_set::end() {
        return sqlite_iterator();
    }

    inline sqlite_result_set::sqlite_result_set(sqlite3 *db, sqlite3_stmt *stmt)
            : _db(db), _stmt(stmt) {
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

    inline sqlite_database::~sqlite_database() noexcept {
        close_internal();
    }

    inline sqlite_database::sqlite_database(sqlite_database &&other) noexcept : _db(other._db),
                                                                                _path(std::move(other._path)) {
        other._db = nullptr;
        other._path = "";
    }

    inline sqlite_database &sqlite_database::operator=(sqlite_database &&other) noexcept {
        _db = other._db;
        _path = std::move(other._path);

        other._db = nullptr;
        other._path = "";

        return *this;
    }

    inline void sqlite_database::open() {
        auto rc = open_internal();
        if (rc != SQLITE_OK) {
            throw sqlite_exception("Failed to open database", rc);
        }
    }

#ifdef SQLITE_HAS_CODEC

    inline void sqlite_database::open(const std::string &passphrase) {
        open();
        sqlite3_key(_db, passphrase.c_str(), static_cast<int>(passphrase.length()));
    }

#endif

    inline void sqlite_database::close() {
        auto rc = close_internal();
        if (rc != SQLITE_OK) {
            throw sqlite_exception("Failed to close database", rc);
        }
    }

    inline void sqlite_database::exec_sql(const std::string &sql) {
        sqlite_statement statement(_db, sql);
        statement.exec();
        statement.finalize();
    }

    template<class... ArgType>
    void sqlite_database::exec_sql(const std::string &sql, ArgType &&... bind_args) {
        sqlite_statement statement(_db, sql);
        statement.bind_values(std::forward<ArgType>(bind_args)...);
        statement.exec();
        statement.finalize();
    }

    inline void sqlite_database::exec(sqlite_statement &statement) {
        statement.reset();
        statement.exec();
    }

    template<class... ArgType>
    void sqlite_database::exec(sqlite_statement &statement, ArgType &&... bind_args) {
        statement.reset();
        statement.clear_bindings();
        statement.bind_values(std::forward<ArgType>(bind_args)...);
        statement.exec();
    }

    inline sqlite_result_set sqlite_database::query(const std::string &sql) {
        sqlite3_stmt *stmt;
        auto rc = sqlite3_prepare_v2(_db, sql.c_str(), static_cast<int>(sql.length()), &stmt, nullptr);
        if (rc != SQLITE_OK) {
            throw sqlite_exception("Failed to prepare statement, SQL = \"" + sql + "\"", rc);
        }
        return sqlite_result_set(_db, stmt);
    }

    template<class... ArgType>
    sqlite_result_set sqlite_database::query(const std::string &sql, ArgType &&... bind_args) {
        sqlite_statement statement(_db, sql);
        //TODO: statement.bind_values(std::forward<ArgType>(bind_args)...);
        return sqlite_result_set(_db, /* TODO: std::move(statement)*/ nullptr);
    }

    inline sqlite_statement sqlite_database::prepare_statement(const std::string &sql) {
        return sqlite_statement(_db, sql);
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

            default:
                std::stringstream ss;
                ss << "Invalid transaction mode, = " << static_cast<int>(mode);
                throw sqlite_exception(ss.str());
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
        return _db != nullptr;
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
            throw sqlite_exception("Invalid version, must be > 0");
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

    inline int sqlite_database::open_internal() noexcept {
        auto rc = sqlite3_open(_path.c_str(), &_db);
        if (rc != SQLITE_OK) {
            sqlite3_close(_db);
            _db = nullptr;
            return rc;
        }

        return rc;
    }

    inline int sqlite_database::close_internal() noexcept {
        if (!_db) {
            return SQLITE_OK;
        }

        sqlite3_stmt *stmt = nullptr;
        while ((stmt = sqlite3_next_stmt(_db, stmt)) != nullptr) {
            auto rc = sqlite3_finalize(stmt);
            if (rc != SQLITE_OK) {
                return rc;
            }
        }

        auto rc = sqlite3_close(_db);
        if (rc == SQLITE_OK) {
            _db = nullptr;
        }
        return rc;
    }
}
