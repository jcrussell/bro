// See the file "COPYING" in the main distribution directory for copyright.

#include "config.h"

#include <string>
#include <errno.h>
#include <vector>

#include "../../NetVar.h"
#include "../../threading/SerialTypes.h"

#include "MySQL.h"

#include <cppconn/statement.h>

using namespace logging;
using namespace writer;
using threading::Value;
using threading::Field;
using namespace std;

MySQL::MySQL(WriterFrontend* frontend)
  : WriterBackend(frontend),
    fields(), num_fields() {
  set_separator.assign(
      (const char*) BifConst::LogMySQL::set_separator->Bytes(),
      BifConst::LogMySQL::set_separator->Len()
      );

  unset_field.assign(
      (const char*) BifConst::LogMySQL::unset_field->Bytes(),
      BifConst::LogMySQL::unset_field->Len()
      );

  empty_field.assign(
      (const char*) BifConst::LogMySQL::empty_field->Bytes(),
      BifConst::LogMySQL::empty_field->Len()
      );

  db_host.assign(
      (const char*) BifConst::LogMySQL::db_host->Bytes(),
      BifConst::LogMySQL::db_host->Len()
      );

  db_name.assign(
      (const char*) BifConst::LogMySQL::db_name->Bytes(),
      BifConst::LogMySQL::db_name->Len()
      );

  db_user.assign(
      (const char*) BifConst::LogMySQL::db_user->Bytes(),
      BifConst::LogMySQL::db_user->Len()
      );

  db_pass.assign(
      (const char*) BifConst::LogMySQL::db_pass->Bytes(),
      BifConst::LogMySQL::db_pass->Len()
      );

  io = new AsciiFormatter(this, AsciiFormatter::SeparatorInfo(set_separator, unset_field, empty_field));
}

MySQL::~MySQL() {
  if (prep_stmt != NULL) {
    delete prep_stmt;
    prep_stmt = NULL;
  }

  if ( con != NULL ) {
    delete con;
    con = NULL;
  }

  delete io;
}

string MySQL::GetTableType(int arg_type, int arg_subtype) {
  string type;

  switch ( arg_type ) {
  case TYPE_BOOL:
    type = "boolean";
    break;

  case TYPE_INT:
  case TYPE_COUNT:
  case TYPE_COUNTER:
  case TYPE_PORT: // note that we do not save the protocol at the moment. Just like in the case of the ascii-writer
    type = "integer";
    break;

  case TYPE_SUBNET:
  case TYPE_ADDR:
    type = "text"; // sqlite3 does not have a type for internet addresses
    break;

  case TYPE_TIME:
  case TYPE_INTERVAL:
  case TYPE_DOUBLE:
    type = "double precision";
    break;

  case TYPE_ENUM:
  case TYPE_STRING:
  case TYPE_FILE:
  case TYPE_FUNC:
    type = "text";
    break;

  case TYPE_TABLE:
  case TYPE_VECTOR:
    type = "text"; // dirty - but sqlite does not directly support arrays. so - we just roll it into a ","-separated string.
    break;

  default:
    Error(Fmt("unsupported field format %d ", arg_type));
    return ""; // not the cleanest way to abort. But sqlite will complain on create table...
  }

  return type;
}

bool MySQL::DoInit(const WriterInfo& info, int arg_num_fields, const Field* const * arg_fields) {
  num_fields = arg_num_fields;
  fields = arg_fields;

  string tablename;

  map<const char*, const char*>::const_iterator it = info.config.find("tablename");
  if ( it == info.config.end() ) {
    Error(Fmt("Tablename configuration option not found."));
    return false;
  }

  tablename = it->second;

  try {
    driver = get_driver_instance();
    con = (sql::mysql::MySQL_Connection*)driver->connect(db_host, db_user, db_pass);
    con->setSchema(db_name);
    sql::Statement *stmt = con->createStatement();

    string create = "CREATE TABLE IF NOT EXISTS " + tablename + " (\n";

    for ( unsigned int i = 0; i < num_fields; ++i ) {
      const Field* field = fields[i];

      if ( i > 0 ) {
        create += ",\n";
      }

      create += con->escapeString(field->name);

      string type = GetTableType(field->type, field->subtype);
      if ( type == "" ) {
        InternalError(Fmt("Could not determine type for field %lu:%s", i, field->name));
        return false;
      }

      create += " " + type;
    }

    stmt -> execute(create);

    // create the prepared statement that will be re-used forever...
    string insert = "INSERT INTO " + tablename + " ( ";
    string values = "VALUES (";

    for ( unsigned int i = 0; i < num_fields; i++ ) {
      if ( i > 0 ) {
        insert += ", ";
        values += ", ";
      }

      values += "?";

      insert += con->escapeString(fields[i]->name);
    }

    insert += ") " + values + ");";

    prep_stmt = con->prepareStatement(insert);

    delete stmt;
  }
  catch (sql::SQLException &e) {
    Error(Fmt("Failed to initialize connection to MySQL: %d", e.getErrorCode()));
    return false;
  }

  return true;
}

// Format String
char* MySQL::FS(const char* format, ...) {
  char* buf;

  va_list al;
  va_start(al, format);
  int n = vasprintf(&buf, format, al);
  va_end(al);

  assert(n >= 0);

  return buf;
}

void MySQL::AddParams(Value* val, int pos) {
  if ( ! val->present ) {
    prep_stmt->setNull(pos, 0);
    return;
  }

  string out;

  switch ( val->type ) {
    case TYPE_BOOL:
      prep_stmt->setInt(pos, val->val.int_val != 0 ? 1 : 0);
      break;

    case TYPE_INT:
      prep_stmt->setInt(pos, val->val.int_val);
      break;

    case TYPE_COUNT:
    case TYPE_COUNTER:
      prep_stmt->setInt(pos, val->val.uint_val);
      break;

    case TYPE_PORT:
      prep_stmt->setInt(pos, val->val.port_val.port);
      break;

    case TYPE_SUBNET:
      out = io->Render(val->val.subnet_val);
      prep_stmt->setString(pos, out);
      break;

    case TYPE_ADDR:
      out = io->Render(val->val.addr_val);
      prep_stmt->setString(pos, out);
      break;

    case TYPE_TIME:
    case TYPE_INTERVAL:
    case TYPE_DOUBLE:
      prep_stmt->setDouble(pos, val->val.double_val);
      break;

    case TYPE_ENUM:
    case TYPE_STRING:
    case TYPE_FILE:
    case TYPE_FUNC:
      if ( ! val->val.string_val.length || val->val.string_val.length == 0 ) {
        prep_stmt->setNull(pos, 0);
        break;
      }

      out = string(val->val.string_val.data, val->val.string_val.length);
      prep_stmt->setString(pos, out);
      break;

    case TYPE_VECTOR: {
      ODesc desc;
      desc.Clear();
      desc.EnableEscaping();
      desc.AddEscapeSequence(set_separator);

      if ( ! val->val.vector_val.size ) {
        desc.Add(empty_field);
      }
      else {
        for ( int j = 0; j < val->val.vector_val.size; j++ ) {
          if ( j > 0 ) {
            desc.AddRaw(set_separator);
          }

          io->Describe(&desc, val->val.vector_val.vals[j], fields[pos]->name);
        }
      }

      desc.RemoveEscapeSequence(set_separator);
      out = string((char*)desc.Bytes(), desc.Len());
      prep_stmt->setString(pos, out);
    }

    default:
      Error(Fmt("unsupported field format %d", val->type));
  }
}

bool MySQL::DoWrite(int num_fields, const Field* const * fields, Value** vals) {
  // bind parameters
  for ( int i = 0; i < num_fields; i++ ) {
    AddParams(vals[i], i+1);
  }

  try {
    prep_stmt->execute();
  }
  catch(sql::SQLException &e) {
    return false;
  }

  return true;
}

