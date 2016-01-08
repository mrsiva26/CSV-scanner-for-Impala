#ifndef IMPALA_EXEC_CSV_SCAN_NODE_H_
#define IMPALA_EXEC_CSV_SCAN_NODE_H_

#include <vector>
#include <memory>
#include <stdint.h>

#include <boost/scoped_ptr.hpp>

#include "exec/exec-node.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "runtime/descriptors.h"
#include "runtime/string-buffer.h"
#include "util/thread.h"

#include "gen-cpp/PlanNodes_types.h"


#include "util/string-parser.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"
#include "runtime/mem-pool.h"
#include "runtime/string-value.inline.h"


namespace impala {

class MemPool;
class SlotDescriptor;
class Status;
class Tuple;
class TupleDescriptor;
class TPlanNode;

/**
 * CSE 5242 - Assignment No. 2: Implement an access method in Impala
 *
 * Node which allows user to read any Comma Separated Value (CSV) file from the local file system
 * and perform operations on it just like any other table. The table schema is defined through a `CREATE EXTERNAL TABLE...` statement as follows:
 *
 *    CREATE EXTERNAL TABLE extest
 *    (
 *       id INT,
 *       col_1 BOOLEAN,
 *       col_2 DOUBLE,
 *       col_3 TIMESTAMP,
 *       col_4 STRING
 *    )
 *    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
 *    LOCATION '/home/cloudera/csv/';
 *
 * Due to current limitations, we have the following restriction:
 * The file that we will read must be named the same as the table name, and must have a .csv extension to it.
 * You will need to save the file under `/home/cloudera/csv`.
 * NOTE: Changing this location does not mean you can move your csv files to that new location. It just won't work for now!
 *
 * Design an appropriate way to read the above CSV file using the iterator based execution model, i.e. Open(), GetNext() and Close().
 * Prepare() has already been implemented here, which populates the Tuple and Slot descriptors with the schema information.
 *
 * On a high level, you will simply be reading rows from a specific file, processing it in a way which allows you to call the WriteSlot()
 * method defined in this file and write the slot data (i.e. the column value) to the slots in the tuple.
 *
 * Eg: Consider the following entry in the file `/home/cloudera/csv/extest.csv` above:
 *
 *        1,true,911.400,2012-10-24 08:55:00,sample data
 *
 *  You are expected to parse this record from the file, and write each column data into a single tuple using the WriteSlot() method.
 *  That constitutes writing one whole tuple of data. This should be done for all the records in the file.
 *  You may need to study the flow of a normal HDFS Scan operation to understand the mechanism behind it.
 *
 *  Feel free to add your own methods or declare variables, but do not change the existing ones, unless you have a very *strong* reason to.
 *  Your solution will be diff'ed from the original problem to see the changes you've made.
 *
 *  Running a query from the Impala shell with this operator for the first time (without any of your modifications) may result into
 *  the query never returning. Press Ctrl^Cto exit the query in that case.
 *  (Sorry about that bad exception handling)
 */
class CsvScanNode : public ExecNode {
 public:
  CsvScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
        int mycount;
  ~CsvScanNode();
  //MemPool* pool;
  // ExecNode methods
  virtual Status Prepare(RuntimeState* state);
  // Create and queue up the rowbatches in the RowBatchQueue
  virtual Status Open(RuntimeState* state);
  // GetNext will call GetNextInternal to dequeue the rowbatch from the RowBatchQueue
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  //close any open I/O buffers that may have been opened to read the file
  virtual void Close(RuntimeState* state);

  // Returns the tuple idx into the row for this scan node to output to.
  // Currently this is always 0.
  int tuple_idx() const { return 0; }

  //used internally during Prepare()
  const static int SKIP_COLUMN = -1;

 private:
  RuntimeState* runtime_state_;

  // Descriptor for tuples this node constructs
  // Check out what tuple details you can infer from this variable
  const TupleDescriptor* tuple_desc_;

  // Tuple id resolved in Prepare() to set tuple_desc_;
  const int tuple_id_;

  //absolute path of the file to read
  const std::string path_;

  // Number of null bytes in the tuple.
  int32_t num_null_bytes_;

  // The tuple memory of batch_.
  uint8_t* tuple_mem_;

  // Current tuple pointer into tuple_mem_.
  Tuple* tuple_;

  // Vector containing slot descriptors for all materialized non-partition key
  // slots. These descriptors are sorted in order of increasing col_pos
  // Eg. For a table having 3 columns: <INT,STRING,BOOLEAN>,
  // the size of this vector will be 3, and materialized_slots_[2] will
  // give you the slot descriptor for the BOOLEAN column.
  // Again, check out what more information you can infer from this variable
  std::vector<SlotDescriptor*> materialized_slots_;

  // Maximum size of materialized_row_batches_.
  int max_materialized_row_batches_;

  // Outgoing row batches queue. Row batches are produced by your operator
  // and consumed by the main thread, which calls the GetNext() of your operator.
  boost::scoped_ptr<RowBatchQueue> materialized_row_batches_;

  // The current row batch being populated.
  RowBatch* batch_;

  // Vector containing indices into materialized_slots_.  The vector is indexed by
  // the slot_desc's col_pos.  Non-materialized slots will have SKIP_COLUMN as its entry.
  std::vector<int> column_idx_to_materialized_slot_idx_;

  // This is the number of io buffers that are owned by the scan node and the scanners.
  // This is used just to help debug leaked io buffers to determine if the leak is
  // happening in the scanners vs other parts of the execution.
  AtomicInt<int> num_owned_io_buffers_;

  // Lock protects access between scanner thread and main query thread (the one calling
  // GetNext()) for all fields below.  If this lock and any other locks needs to be taken
  // together, this lock must be taken first.
  boost::mutex lock_;

  // Flag signaling that all scanner threads are done.  This could be because they
  // are finished, an error/cancellation occurred, or the limit was reached.
  // Setting this to true triggers the scanner threads to clean up.
  // This should not be explicitly set. Instead, call SetDone().
  bool done_;

  //Method which returns the absolute path of the file that you have to read
  inline std::string GetFilePath(){return path_;}

  // Checks for eos conditions and returns batches from materialized_row_batches_.
  Status GetNextInternal(RuntimeState* state, RowBatch* row_batch, bool* eos);

  // Set batch_ to a new row batch and update tuple_mem_ accordingly.
  void StartNewRowBatch();

  // Gets memory for outputting tuples into batch_.
  // *pool is the mem pool that should be used for memory allocated for those tuples.
  // *tuple_mem should be the location to output tuples, and
  // *tuple_row_mem for outputting tuple rows.
  // Returns the maximum number of tuples/tuple rows that can be output (before the
  // current row batch is complete and a new one is allocated).
  // Memory returned from this call is invalidated after calling CommitRows.  Callers must
  // call GetMemory again after calling this function.
  int GetMemory(MemPool** pool, Tuple** tuple_mem, TupleRow** tuple_row_mem);

  /**
   * Converts slot data, of length 'len',  into type of slot_desc,
   * and writes the result into the tuples's slot.
   * String data is always copied before being written to the slot.
   * The memory for this copy is allocated from 'pool', otherwise
   * 'pool' is unused.
   * Unsuccessful conversions are turned into NULLs.
   * Returns true if the value was written successfully.
   */
  inline bool WriteSlot(const SlotDescriptor* slot_desc, Tuple* tuple,
      const char* data, int len, MemPool* pool) {
        //LOG(INFO)<<"USER PRINT ENTERED WRITESLOT";

    if ((len == 0 && slot_desc->type().type != TYPE_STRING) || data == NULL) {
      tuple->SetNull(slot_desc->null_indicator_offset());
      return true;
    }

    StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
    //LOG(INFO)<<"USER PRINT PARSE SUCCESS IN WRITESLOT";

        void* slot = tuple->GetSlot(slot_desc->tuple_offset());

    // Parse the raw-text data. Translate the text string to internal format.
    switch (slot_desc->type().type) {
      case TYPE_STRING: {
      std::string s(data);
      const char* val = s.c_str();
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        str_slot->ptr = const_cast<char*>(val);
        str_slot->len = len;
      if (len != 0) {
        DCHECK(pool != NULL);
        LOG(INFO)<<"USER PRINT AFTER DHCECK IN WRITESLOT";
        char* slot_data = reinterpret_cast<char*>(pool->Allocate(len));
        memcpy(slot_data, data, str_slot->len);
        str_slot->ptr = slot_data;
      }
        break;
      }
      case TYPE_BOOLEAN:
        *reinterpret_cast<bool*>(slot) =
          StringParser::StringToBool(data, len, &parse_result);
        break;
      case TYPE_TINYINT:
        *reinterpret_cast<int8_t*>(slot) =
          StringParser::StringToInt<int8_t>(data, len, &parse_result);
        break;
      case TYPE_SMALLINT:
        *reinterpret_cast<int16_t*>(slot) =
          StringParser::StringToInt<int16_t>(data, len, &parse_result);
        break;
      case TYPE_INT:
        *reinterpret_cast<int32_t*>(slot) =
          StringParser::StringToInt<int32_t>(data, len, &parse_result);
        break;
      case TYPE_BIGINT:
        *reinterpret_cast<int64_t*>(slot) =
          StringParser::StringToInt<int64_t>(data, len, &parse_result);
        break;
      case TYPE_FLOAT:
        *reinterpret_cast<float*>(slot) =
          StringParser::StringToFloat<float>(data, len, &parse_result);
        break;
      case TYPE_DOUBLE:
        *reinterpret_cast<double*>(slot) =
          StringParser::StringToFloat<double>(data, len, &parse_result);
        break;
      case TYPE_TIMESTAMP: {
        TimestampValue* ts_slot = reinterpret_cast<TimestampValue*>(slot);
        *ts_slot = TimestampValue(data, len);
        if (!ts_slot->HasDateOrTime()) {
          parse_result = StringParser::PARSE_FAILURE;
        }
        break;
      }
      case TYPE_DECIMAL: {
        switch (slot_desc->slot_size()) {
          case 4:
            *reinterpret_cast<Decimal4Value*>(slot) =
                StringParser::StringToDecimal<int32_t>(
                    data, len, slot_desc->type(), &parse_result);
            break;
          case 8:
            *reinterpret_cast<Decimal8Value*>(slot) =
                StringParser::StringToDecimal<int64_t>(
                    data, len, slot_desc->type(), &parse_result);
            break;
          case 12:
            DCHECK(false) << "Planner should not generate this.";
            break;
          case 16:
            *reinterpret_cast<Decimal16Value*>(slot) =
                StringParser::StringToDecimal<int128_t>(
                    data, len, slot_desc->type(), &parse_result);
            break;
          default:
            DCHECK(false) << "Decimal slots can't be this size.";
        }
        if (parse_result != StringParser::PARSE_SUCCESS) {
          // Don't accept underflow and overflow for decimals.
          parse_result = StringParser::PARSE_FAILURE;
        }
        break;
      }
      default:
        DCHECK(false) << "bad slot type: " << slot_desc->type();
        break;
    }

    // TODO: add warning for overflow case
    if (parse_result == StringParser::PARSE_FAILURE) {
      tuple->SetNull(slot_desc->null_indicator_offset());
      return false;
    }

    return true;
  }

  // Initialize a tuple.
  // You may use this method to initialize the tuple before materializing it,
  // i.e. before passing the data to WriteSlot()
  void InitTuple(Tuple* tuple) {
        LOG(INFO)<<"USER PRINT ENTERRED INIT TUPLE";
        memset(tuple, 0, sizeof(uint8_t) * num_null_bytes_);
        LOG(INFO)<<"USER PRINT EXITING INIT TUPLE";

 }

  // Commit num_rows to the current row batch.  If this completes the row batch, the
  // row batch is enqueued with the scan node and StartNewRowBatch is called.
  // Returns Status::OK if the query is not cancelled and hasn't exceeded any mem limits.
  Status CommitRows(int num_rows);

  // Commit num_rows to the current row batch.  The row batch is enqueued
  // with the scan node. Does NOT call StartNewRowBatch(). Can be used to commit last set of records.
  Status CommitLastRows(int num_rows);

  // sets done_ to true and triggers a cleanup. Cannot be called with
  // any locks taken. Calling it repeatedly ignores subsequent calls.
  // This should ideally be called every time your materialized_row_batches_ is filled completely
  // or there are no more records to read from the file.
  void SetDone();
};

}

#endif