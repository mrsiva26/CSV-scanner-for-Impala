#include "exec/csv-scan-node.h"

#include <fstream>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <sstream>
#include <string.h>
#include <boost/tokenizer.hpp>
#include "runtime/runtime-state.h"
#include "runtime/row-batch.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/runtime-profile.h"
#include "gen-cpp/PlanNodes_types.h"
#include "exprs/expr-context.h"
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>
#include "runtime/exec-env.h"
#include "statestore/statestore-subscriber.h"
#include <boost/mpl/vector.hpp>

using namespace impala;
using namespace std;
using namespace boost;


CsvScanNode::CsvScanNode(ObjectPool* pool, const TPlanNode& tnode,
                const DescriptorTbl& descs) :
                ExecNode(pool, tnode, descs), runtime_state_(NULL), tuple_desc_(NULL), tuple_id_(
                                tnode.csv_scan_node.tuple_id), path_(tnode.path), tuple_(NULL), batch_(
                                NULL), done_(false) {
                                                                        LOG(INFO)<<"\nUSER PRINT INSIDE CONSTRUCTOR.cc";
        max_materialized_row_batches_ = 10 * DiskInfo::num_disks();
        materialized_row_batches_.reset(
                        new RowBatchQueue(max_materialized_row_batches_));
        mycount=0;
}

CsvScanNode::~CsvScanNode() {
        LOG(INFO)<<"\nUSER PRINT INSIDE DESTRUCTOR.cc";
}

Status CsvScanNode::Prepare(RuntimeState* state) {
                LOG(INFO)<<"\nUSER PRINT INSIDE PREPARE";
        runtime_state_ = state;
        RETURN_IF_ERROR(ExecNode::Prepare(state));

        tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
        DCHECK(tuple_desc_ != NULL);

        num_null_bytes_ = tuple_desc_->num_null_bytes();

        // Create mapping from column index in table to slot index in output tuple.
        // First, initialize all columns to SKIP_COLUMN.
        int num_cols = tuple_desc_->table_desc()->num_cols();
        column_idx_to_materialized_slot_idx_.resize(num_cols);
        for (int i = 0; i < num_cols; ++i) {
                column_idx_to_materialized_slot_idx_[i] = SKIP_COLUMN;
        }

        // Next, collect all materialized slots
        vector<SlotDescriptor*> all_materialized_slots;
        all_materialized_slots.resize(num_cols);
        const vector<SlotDescriptor*>& slots = tuple_desc_->slots();
        for (size_t i = 0; i < slots.size(); ++i) {
                if (!slots[i]->is_materialized())
                        continue;
                int col_idx = slots[i]->col_pos();
                DCHECK_LT(col_idx, column_idx_to_materialized_slot_idx_.size());
                DCHECK_EQ(column_idx_to_materialized_slot_idx_[col_idx], SKIP_COLUMN);
                all_materialized_slots[col_idx] = slots[i];
        }

        // Finally, populate materialized_slots_ in the order that
        // the slots appear in the file.
        for (int i = 0; i < num_cols; ++i) {
                SlotDescriptor* slot_desc = all_materialized_slots[i];
                if (slot_desc == NULL)
                        continue;
                else {
                        column_idx_to_materialized_slot_idx_[i] =
                                        materialized_slots_.size();
                        materialized_slots_.push_back(slot_desc);
                }
        }

        return Status::OK;
}

void CsvScanNode::StartNewRowBatch() {
                LOG(INFO)<<"\nUSER PRINT INSIDE StartNewRowBatch";
        batch_ = new RowBatch(row_desc(), runtime_state_->batch_size(),
                        mem_tracker());
        tuple_mem_ = batch_->tuple_data_pool()->Allocate(
                        runtime_state_->batch_size() * tuple_desc_->byte_size());
}

std::string ReplaceAll(std::string str, const std::string& from, const std::string& to) {
    size_t start_pos = 0;
    while((start_pos = str.find(from, start_pos)) != std::string::npos) {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length(); // Handles case where 'to' is a substring of 'from'
    }
    return str;
}
Status CsvScanNode::Open(RuntimeState* state) {
                LOG(INFO)<<"\nUSER PRINT INSIDE OPEN";
        RETURN_IF_ERROR(ExecNode::Open(state));
        StartNewRowBatch();

        MemPool* pool;
        TupleRow* tuple_row;
        LOG(INFO)<<"I am line before GetMemory";
        GetMemory(&pool, &tuple_, &tuple_row);

        uint8_t* tuple_row_mem = reinterpret_cast<uint8_t*>(tuple_row);
        uint8_t* tuple_mem = reinterpret_cast<uint8_t*>(tuple_);

        Tuple* tuple = reinterpret_cast<Tuple*>(tuple_mem);
        InitTuple(tuple);


        ifstream file(GetFilePath().c_str());
        std::string linebuffer;
        if(file!=NULL)
        LOG(INFO)<<"FILE OPENED";
		int j=0;
       while(file && getline(file,linebuffer) ){
		j++;
        linebuffer = ReplaceAll(linebuffer, std::string("\\,"), std::string(""));
        std::stringstream lineStream(linebuffer);
                int i=0;
        std::string cell;
                while(std::getline(lineStream,cell,','))
                {
                cell=ReplaceAll(cell, std::string("\\,"), std::string(""));;
                //id="1";
                WriteSlot(materialized_slots_[i], tuple, cell.c_str(), cell.length(), pool);
                LOG(INFO)<<i<<" writeslots completed";
                i++;
                }
                tuple_mem += tuple_desc_->byte_size();
                tuple_row_mem += batch_->row_byte_size();
                tuple = reinterpret_cast<Tuple*>(tuple_mem);
                tuple_row = reinterpret_cast<TupleRow*>(tuple_row_mem);
                InitTuple(tuple);

        }

        Status status2 = CommitLastRows(j);
        return Status::OK;
}

// Do not change this method
int CsvScanNode::GetMemory(MemPool** pool, Tuple** tuple_mem,
                TupleRow** tuple_row_mem) {
        LOG(INFO)<<"\nUSER PRINT INSIDE GETMEMEORY";
                DCHECK(batch_ != NULL);
        DCHECK(!batch_->AtCapacity());
        *pool = batch_->tuple_data_pool();
        LOG(INFO)<<"POOL VALUE: "<<pool<<" endl";
        *tuple_mem = reinterpret_cast<Tuple*>(tuple_mem_);
        *tuple_row_mem = batch_->GetRow(batch_->AddRow());
        return batch_->capacity() - batch_->num_rows();
}

Status CsvScanNode::GetNext(RuntimeState* state, RowBatch* row_batch,
                bool* eos) {
                LOG(INFO)<<"\nUSER PRINT INSIDE GETNEXT";
        Status status = GetNextInternal(state, row_batch, eos);
        if (status.IsMemLimitExceeded())
                state->SetMemLimitExceeded();
        SetDone();
        return status;

}

Status CsvScanNode::GetNextInternal(RuntimeState* state, RowBatch* row_batch,
                bool* eos) {
        LOG(INFO)<<"\nUSER PRINT INSIDE GetNextInternal";
        RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
        //LOG(INFO)<<"\nUSER PRINT ExecDebugAction";
        RETURN_IF_CANCELLED(state);
        //LOG(INFO)<<"\nUSER PRINT CANCELLED";
        RETURN_IF_ERROR(state->CheckQueryState());
        //LOG(INFO)<<"\nUSER PRINT CheckQueryState";
        SCOPED_TIMER(runtime_profile_->total_time_counter());
        //LOG(INFO)<<"\nUSER PRINT Timer";
        /*
        MemPool* pool;
        TupleRow* tuple_row;
        LOG(INFO)<<"I am line before GetMemory";
        GetMemory(&pool, &tuple_, &tuple_row);

        uint8_t* tuple_row_mem = reinterpret_cast<uint8_t*>(tuple_row);
        uint8_t* tuple_mem = reinterpret_cast<uint8_t*>(tuple_);

        Tuple* tuple = reinterpret_cast<Tuple*>(tuple_mem);
        InitTuple(tuple);

        string id;
        id="1";
        WriteSlot(materialized_slots_[0], tuple, id.c_str(), 1, pool);
        LOG(INFO)<<"1 writeslots completed";
        id="true";
        WriteSlot(materialized_slots_[1], tuple, id.c_str(), 4, pool);
        LOG(INFO)<<"2 writeslots completed";
        id="123.123";
        WriteSlot(materialized_slots_[2], tuple, id.c_str(), 7, pool);
        LOG(INFO)<<"3 writeslots completed";
        id="2012-10-24 08:55:00";
        WriteSlot(materialized_slots_[3], tuple, id.c_str(), 19, pool);
        LOG(INFO)<<"4 writeslots completed";
        id="voldemort";
        WriteSlot(materialized_slots_[4], tuple, id.c_str(), 9, pool);
        LOG(INFO)<<"5 writeslots completed";



        tuple_mem += tuple_desc_->byte_size();
        tuple_row_mem += batch_->row_byte_size();
        tuple = reinterpret_cast<Tuple*>(tuple_mem);
        tuple_row = reinterpret_cast<TupleRow*>(tuple_row_mem);

        ExprContext* const* ctxs = &conjunct_ctxs_[0];
        LOG(INFO)<<"CONJUNCTS OBTAINED";
        CommitLastRows(1);
        */

        //if(!EvalConjuncts(ctxs, conjunct_ctxs_.size(), tuple_row))
        //LOG(INFO)<<"SATISFIES CONJUNCT";


        /**
        *By this time, the materialized_row_batches_ must already be populated
        *through a call to either CommitRows() or CommitLastRows().
        **/

        RowBatch* materialized_batch = materialized_row_batches_->GetBatch();
        if (materialized_batch != NULL) {
                num_owned_io_buffers_ -= materialized_batch->num_io_buffers();
                row_batch->AcquireState(materialized_batch);
                // Update the number of materialized rows instead of when they are materialized.
                // This means that scanners might process and queue up more rows than are necessary
                // for the limit case but we want to avoid the synchronized writes to
                // num_rows_returned_
                num_rows_returned_ += row_batch->num_rows();
                COUNTER_SET(rows_returned_counter_, num_rows_returned_);

                if (ReachedLimit()) {
                        int num_rows_over = num_rows_returned_ - limit_;
                        row_batch->set_num_rows(row_batch->num_rows() - num_rows_over);
                        num_rows_returned_ -= num_rows_over;
                        COUNTER_SET(rows_returned_counter_, num_rows_returned_);

                        *eos = true;
                        SetDone();
                }
                DCHECK_EQ(materialized_batch->num_io_buffers(), 0);
                delete materialized_batch;
                *eos = false;
                return Status::OK;
        }

        *eos = true;
        LOG(INFO)<<"\nUSER PRINT RETURN GETINTERNALNEXT";
        return Status::OK;
}

void CsvScanNode::Close(RuntimeState* state) {
        LOG(INFO)<<"\nUSER PRINT INSIDE close";
                if (is_closed())
                return;
        SetDone();

        //housekeeping tasks
        num_owned_io_buffers_ -= materialized_row_batches_->Cleanup();
        DCHECK_EQ(num_owned_io_buffers_, 0) << "ScanNode has leaked io buffers";
        //materialized_row_batches_.reset();
        ExecNode::Close(state);
}

// Do not change this method
void CsvScanNode::SetDone() {
                LOG(INFO)<<"\nUSER PRINT INSIDE sETdONE";
        {
                unique_lock<mutex> l(lock_);
                if (done_)
                        return;
                done_ = true;
        }
        materialized_row_batches_->Shutdown();
}

  // Commit num_rows to the current row batch.  If this completes the row batch, the
  // row batch is enqueued with the scan node and StartNewRowBatch is called.
  // Returns Status::OK if the query is not cancelled and hasn't exceeded any mem limits.
  // Scanner can call this with 0 rows to flush any pending resources (attached pools
  // and io buffers) to minimize memory consumption.
  // Do not change this method
Status CsvScanNode::CommitRows(int num_rows) {
                LOG(INFO)<<"\nUSER PRINT INSIDE CommitRows";
        DCHECK(batch_ != NULL);
        DCHECK_LE(num_rows, batch_->capacity() - batch_->num_rows());
        batch_->CommitRows(num_rows);
        tuple_mem_ += tuple_desc_->byte_size() * num_rows;

        if (batch_->AtCapacity()) {
                materialized_row_batches_->AddBatch(batch_);
                StartNewRowBatch();
        }

        return Status::OK;
}

// Do not change this method
Status CsvScanNode::CommitLastRows(int num_rows) {
                LOG(INFO)<<"\nUSER PRINT INSIDE CommitLastRows";
        DCHECK(batch_ != NULL);
        DCHECK_LE(num_rows, batch_->capacity() - batch_->num_rows());
        batch_->CommitRows(num_rows);
        tuple_mem_ += tuple_desc_->byte_size() * num_rows;

        materialized_row_batches_->AddBatch(batch_);
        return Status::OK;
}