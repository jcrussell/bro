// See the file "COPYING" in the main distribution directory for copyright.
//
// Log writer for MYSQL logs.

#ifndef LOGGING_WRITER_MYSQL_H
#define LOGGING_WRITER_MYSQL_H

#include "config.h"

#include "../WriterBackend.h"

#include "threading/AsciiFormatter.h"

#include "mysql_connection.h"

#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/prepared_statement.h>

namespace logging { namespace writer {

class MySQL : public WriterBackend {
public:
  MySQL(WriterFrontend* frontend);
  ~MySQL();

  static WriterBackend* Instantiate(WriterFrontend* frontend)
    { return new MySQL(frontend); }

protected:
  virtual bool DoInit(const WriterInfo& info, int arg_num_fields,
          const threading::Field* const* arg_fields);
  virtual bool DoWrite(int num_fields, const threading::Field* const* fields,
           threading::Value** vals);
  virtual bool DoSetBuf(bool enabled) { return true; }
  virtual bool DoRotate(const char* rotated_path, double open,
            double close, bool terminating) { return true; }
  virtual bool DoFlush(double network_time)  { return true; }
  virtual bool DoFinish(double network_time)  { return true; }
  virtual bool DoHeartbeat(double network_time, double current_time)  { return true; }

private:
  void AddParams(threading::Value* val, int pos);
  string GetTableType(int, int);
  char* FS(const char* format, ...);

  const threading::Field* const * fields; // raw mapping
  unsigned int num_fields;

  sql::Driver *driver;
  sql::mysql::MySQL_Connection *con;
  sql::PreparedStatement *prep_stmt;

  string set_separator;
  string unset_field;
  string empty_field;

  string db_host;
  string db_name;
  string db_user;
  string db_pass;

  AsciiFormatter* io;
};

}
}

#endif /* LOGGING_WRITER_MYSQL_H */

