/*// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/pretty_print.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>

#include <iostream>

using arrow::Status;

namespace {

Status RunMain(int argc, char** argv) {
const char* csv_filename = "test.csv";
const char* arrow_filename = "test.arrow";

std::cerr << "* Reading CSV file '" << csv_filename << "' into table" << std::endl;
ARROW_ASSIGN_OR_RAISE(auto input_file, arrow::io::ReadableFile::Open(csv_filename));
ARROW_ASSIGN_OR_RAISE(auto csv_reader, arrow::csv::TableReader::Make(
arrow::io::default_io_context(), input_file,
arrow::csv::ReadOptions::Defaults(),
arrow::csv::ParseOptions::Defaults(),
arrow::csv::ConvertOptions::Defaults()));
ARROW_ASSIGN_OR_RAISE(auto table, csv_reader->Read());

std::cerr << "* Read table:" << std::endl;
ARROW_RETURN_NOT_OK(arrow::PrettyPrint(*table, {}, &std::cerr));

std::cerr << "* Writing table into Arrow IPC file '" << arrow_filename << "'"
          << std::endl;
ARROW_ASSIGN_OR_RAISE(auto output_file,
                      arrow::io::FileOutputStream::Open(arrow_filename));
ARROW_ASSIGN_OR_RAISE(auto batch_writer,
                      arrow::ipc::MakeFileWriter(output_file, table->schema()));
ARROW_RETURN_NOT_OK(batch_writer->WriteTable(*table));
ARROW_RETURN_NOT_OK(batch_writer->Close());

return Status::OK();
}

}  // namespace

int main(int argc, char** argv) {
Status st = RunMain(argc, argv);
if (!st.ok()) {
std::cerr << st << std::endl;
return 1;
}
return 0;
}*/

#include <arrow/api.h>
#include <iostream>


arrow::Status RunMain(){
    //create arrow array from c++ array
    arrow::Int8Builder int8builder;
    int8_t days_raw[5] = {1,2,3,4,5};
    ARROW_RETURN_NOT_OK(int8builder.AppendValues(days_raw,5)); //return bad arrow status if adding values did not work
    // build the arrow array
    std::shared_ptr<arrow::Array> days;
    ARROW_ASSIGN_OR_RAISE(days,int8builder.Finish()); //get arrow array & reset & return on bad arrow status..
    // build some more arrays for next concept
    int8_t months_raw[5] = {11,12,1,2,4};
    ARROW_RETURN_NOT_OK(int8builder.AppendValues(months_raw,5)); //return bad arrow status if adding values did not work
    std::shared_ptr<arrow::Array> months;
    ARROW_ASSIGN_OR_RAISE(months,int8builder.Finish()); //get arrow array & reset & return on bad arrow status..
    // build some more using diff type
    arrow::Int16Builder int16builder;
    int16_t years_raw[5] = {1911,1932,2011,2032,1999};
    ARROW_RETURN_NOT_OK(int16builder.AppendValues(years_raw,5)); //return bad arrow status if adding values did not work
    std::shared_ptr<arrow::Array> years;
    ARROW_ASSIGN_OR_RAISE(years,int16builder.Finish()); //get arrow array & reset & return on bad arrow status..
    

    //create a recordbatch
    // create structure
    std::shared_ptr<arrow::Field> field_day, field_month, field_year;
    std::shared_ptr<arrow::Schema> schema;
    field_day = arrow::field("Day",arrow::int8()); //create(copy) field via ns function
    field_month = arrow::field("Month",arrow::int8());
    field_year = arrow::field("Year",arrow::int16());
    schema = arrow::schema({field_day,field_month,field_year}); //a date(rec) schema, or: a table entry
    // insert data using arrow arrays
    std::shared_ptr<arrow::RecordBatch> record_batch;
    record_batch = arrow::RecordBatch::Make(schema,5,{days,months,years}); //table of dates
    // print
    std::cout << record_batch->ToString() << std::endl;
    //
    return arrow::Status::OK();
}



int main(){
    
    arrow::Status st = RunMain();
    if(!st.ok()){
        std::cerr<<st<<std::endl;
        return 1;
    }
    return 0;
}