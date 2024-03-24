#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <arrow/compute/api.h>

#include <gandiva/node.h>
#include <gandiva/condition.h>
#include <gandiva/expression.h>
#include <gandiva/tree_expr_builder.h>
#include <gandiva/projector.h>
#include <gandiva/filter.h>

#include <iostream>
#include <sstream>
#include <vector>

#include "arrow_custom_kernel.cpp"

arrow::Status GenInitialFile(){
    //create arrow array from c++ array
    arrow::Int8Builder int8builder;
    int8_t days_raw[5] = {1,2,3,4,5};
    ARROW_RETURN_NOT_OK(int8builder.AppendValues(days_raw,5)); //return bad arrow status if adding values did not work
    // build the arrow array
    S(arrow::Array,days);
    ARROW_ASSIGN_OR_RAISE(days,int8builder.Finish()); //get arrow array & reset & return on bad arrow status..
    // build some more arrays
    int8_t months_raw[5] = {11,12,1,2,4};
    ARROW_RETURN_NOT_OK(int8builder.AppendValues(months_raw,5)); //return bad arrow status if adding values did not work
    S(arrow::Array,months);
    ARROW_ASSIGN_OR_RAISE(months,int8builder.Finish());
    // build some more using diff type
    arrow::Int16Builder int16builder;
    int16_t years_raw[5] = {1911,1932,2011,2032,1999};
    ARROW_RETURN_NOT_OK(int16builder.AppendValues(years_raw,5)); //return bad arrow status if adding values did not work
    S(arrow::Array,years);
    ARROW_ASSIGN_OR_RAISE(years,int16builder.Finish());

    //create a recordbatch
    // create structure
    S(arrow::Field,field_day) = arrow::field("Day",arrow::int8()); //create(copy) field via ns function
    S(arrow::Field,field_month) = arrow::field("Month",arrow::int8());
    S(arrow::Field,field_year) = arrow::field("Year",arrow::int16());
    S(arrow::Schema,schema) = arrow::schema({field_day,field_month,field_year}); //a date(rec) schema, or: a table entry
    // insert data using arrow arrays
    S(arrow::RecordBatch,record_batch) = arrow::RecordBatch::Make(schema,5,{days,months,years}); //table of dates
    // print
    std::cout << record_batch->ToString() << std::endl;
    
    
    //create chunkedarray
    // create origin arrays using the already ex. builders
    int8_t days_raw2[5] = {1,2,3,4,5};
    ARROW_RETURN_NOT_OK(int8builder.AppendValues(days_raw2,5));
    ARROW_ASSIGN_OR_RAISE(S(arrow::Array,days2), int8builder.Finish());

    int8_t months_raw2[5] = {2,2,1,4,1};
    ARROW_RETURN_NOT_OK(int8builder.AppendValues(months_raw2,5));
    ARROW_ASSIGN_OR_RAISE(S(arrow::Array,months2), int8builder.Finish());
    
    int16_t years_raw2[5] = {1999,2000,2001,2003,2004};
    ARROW_RETURN_NOT_OK(int16builder.AppendValues(years_raw2,5));
    ARROW_ASSIGN_OR_RAISE(S(arrow::Array,years2),int16builder.Finish());
    
    //manually creating arrays without copying data:
    uint8_t my_data[16]; //theres a required min length
    my_data[0] = 42;my_data[1] = 44;
    std::vector<std::shared_ptr<arrow::Buffer>> bufs(2); //NOTE: not inc validity buffer
    bufs[1] = arrow::Buffer::Wrap<uint8_t>(my_data,16); //databuffer
    S(arrow::ArrayData,arraydata) = arrow::ArrayData::Make(arrow::int8(),16,bufs);
    arrow::NumericArray<arrow::Int8Type> arr(arraydata);//copy for print
    std::cout<<arr.ToString()<<std::endl; 

    //create "placeholder" containers
    arrow::ArrayVector day_vecs{days,days2}; //internal array of array container 
    
    //create chunked array
    // - understandable as some kind of cheap concatenated array 
    M(arrow::ChunkedArray,day_chunks,(day_vecs));

    // Repeat for months.
    arrow::ArrayVector month_vecs{months, months2};
    M(arrow::ChunkedArray,month_chunks,(month_vecs));

    // Repeat for years.
    arrow::ArrayVector year_vecs{years, years2};
    M(arrow::ChunkedArray,year_chunks,(year_vecs));

    //create tables:
    // - recordbatch created from array
    // - table created from chunkedarray
    // - both use same schema for describing columns
    // -> recordbatch has limited rowsize since arraysize limited
    // -> table can be bigger but doesnt guarantee columns in columnar format
    S(arrow::Table,table) = arrow::Table::Make(schema,{day_chunks,month_chunks,year_chunks});
    
    //reading table entries:
    S(arrow::ChunkedArray,column) = table->GetColumnByName("Day");
    ARROW_ASSIGN_OR_RAISE(S(arrow::Scalar,cell), column->GetScalar(7));
    std::cout<<"8. cell of column 'day': "<<cell->ToString()<<std::endl;
    
    // Write out test files in IPC, CSV, and Parquet for the example to use.
    ARROW_ASSIGN_OR_RAISE(S(arrow::io::FileOutputStream,outfile), arrow::io::FileOutputStream::Open(BUILDDIR "/test_in.arrow"));
    ARROW_ASSIGN_OR_RAISE(S(arrow::ipc::RecordBatchWriter,ipc_writer),arrow::ipc::MakeFileWriter(outfile, schema));
    ARROW_RETURN_NOT_OK(ipc_writer->WriteTable(*table));
    ARROW_RETURN_NOT_OK(ipc_writer->Close());

    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(BUILDDIR "/test_in.csv"));
    ARROW_ASSIGN_OR_RAISE(auto csv_writer,arrow::csv::MakeCSVWriter(outfile, table->schema()));
    ARROW_RETURN_NOT_OK(csv_writer->WriteTable(*table));
    ARROW_RETURN_NOT_OK(csv_writer->Close());

    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(BUILDDIR "/test_in.parquet"));
    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, 5));


    return arrow::Status::OK();
}

arrow::Status ReadAndWriteStuff()
{
    ARROW_RETURN_NOT_OK(GenInitialFile());
    
    //read a file (feather format)
    // create an object for file input
    ARROW_ASSIGN_OR_RAISE(S(arrow::io::ReadableFile,inFile), arrow::io::ReadableFile::Open(BUILDDIR "/test_in.arrow",arrow::default_memory_pool()));
    // use a more concrete reader to read from the container
    // feather format is ipc saved as file 
    ARROW_ASSIGN_OR_RAISE(S(arrow::ipc::RecordBatchFileReader,ipc_reader),arrow::ipc::RecordBatchFileReader::Open(inFile));
    // get contents into a recordbatch
    ARROW_ASSIGN_OR_RAISE(S(arrow::RecordBatch,recordbatch), ipc_reader->ReadRecordBatch(0)); //there can be many recordbatches in a file
    //write a file (feather format)
    // bind the object to the file to be written to
    ARROW_ASSIGN_OR_RAISE(S(arrow::io::FileOutputStream,outFile), arrow::io::FileOutputStream::Open(BUILDDIR "/test_out.arrow"));
    // create an object responsible for writing spec arrow ds
    // in this case, use a recordbatch because we want to write the just read recordbatch
    // in order to create a recordbatch writer we need the output file obj and the schema,
    //  use just created obj file and schema from read recordbatch
    ARROW_ASSIGN_OR_RAISE(S(arrow::ipc::RecordBatchWriter,ipc_writer),arrow::ipc::MakeFileWriter(outFile,recordbatch->schema()));
    // write contents to physical file:
    ARROW_RETURN_NOT_OK(ipc_writer->WriteRecordBatch(*recordbatch));
    // close file (ipc writers special use case)
    ARROW_RETURN_NOT_OK(ipc_writer->Close());

    //read a (csv) file
    //reuse input and output file objs
    ARROW_ASSIGN_OR_RAISE(inFile, arrow::io::ReadableFile::Open(BUILDDIR "/test_in.csv"));
    // create a csv reader with default options
    S(arrow::csv::TableReader,csv_reader);
    ARROW_ASSIGN_OR_RAISE(csv_reader, arrow::csv::TableReader::Make(
        arrow::io::default_io_context(),
        inFile,
        arrow::csv::ReadOptions::Defaults(),
        arrow::csv::ParseOptions::Defaults(),
        arrow::csv::ConvertOptions::Defaults()));
    // read actual contents
    ARROW_ASSIGN_OR_RAISE(S(arrow::Table,csv_table), csv_reader->Read());
    //write a (csv) file
    // get output file obj, use already ex obj
    ARROW_ASSIGN_OR_RAISE(outFile,arrow::io::FileOutputStream::Open(BUILDDIR "/test_out.csv"));
    // create writer, dont ask why its now also the ipc recordbatch writer
    ARROW_ASSIGN_OR_RAISE(S(arrow::ipc::RecordBatchWriter,csv_writer),arrow::csv::MakeCSVWriter(outFile,csv_table->schema()));
    // write content
    ARROW_RETURN_NOT_OK(csv_writer->WriteTable(*csv_table));
    // close file
    ARROW_RETURN_NOT_OK(csv_writer->Close());

    //read a (parquet) file
    // get input file
    ARROW_ASSIGN_OR_RAISE(inFile, arrow::io::ReadableFile::Open(BUILDDIR "/test_in.parquet"));
    // get parquet reader (note: extern dependency)
    U(parquet::arrow::FileReader,pReader);
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(inFile,arrow::default_memory_pool(), &pReader));
    // create table ptr and read contents into arrow table using the parquet reader
    S(arrow::Table,pTable);
    PARQUET_THROW_NOT_OK(pReader->ReadTable(&pTable));
    //writing a (parquet) file
    // get output file
    ARROW_ASSIGN_OR_RAISE(outFile, arrow::io::FileOutputStream::Open(BUILDDIR "/test_out.parquet"));
    // write without writer obj
    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*pTable,arrow::default_memory_pool(),outFile,5));

    return arrow::Status::OK();
}

arrow::Status ComputeStuff()
{
    //create some a table with two columns each 5 rows
    arrow::Int32Builder i32b;
    int32_t n1_raw[5] = {42,24,666,111,9876};
    int32_t n2_raw[5] = {4,1999,2023,777,6};
    ARROW_RETURN_NOT_OK(i32b.AppendValues(n1_raw,5));
    ARROW_ASSIGN_OR_RAISE(S(arrow::Array,n1), i32b.Finish());
    ARROW_RETURN_NOT_OK(i32b.AppendValues(n2_raw,5));
    ARROW_ASSIGN_OR_RAISE(S(arrow::Array,n2), i32b.Finish());
    S(arrow::Field,field_n1) = arrow::field("n1",arrow::int32());
    S(arrow::Field,field_n2) = arrow::field("n2",arrow::int32());
    S(arrow::Schema,schema) = arrow::schema({field_n1,field_n2});
    S(arrow::Table,table) = arrow::Table::Make(schema,{n1,n2},5);

    //Aggregationen, Element-weise-Funktionen, Array-weise-Funktionen
    //calc sum over an array
    // create result obj
    // very generic object holding every type of result
    arrow::Datum sum_n1;
    // call arrow conv func
    ARROW_ASSIGN_OR_RAISE(sum_n1,arrow::compute::Sum({table->GetColumnByName("n1")}));
    //ARROW_ASSIGN_OR_RAISE(sum_n1,arrow::compute::CallFunction("sum",{table->GetColumnByName("n1")}));
    // get results
    // print type of result
    std::cout<<"sum_n1 Datum is of kind'"
        <<sum_n1.ToString()
        <<"' and type of content '"
        <<sum_n1.type()->ToString()<<"'"
        <<std::endl;
    // print the actual result contents by requesting the contents as a specific type
    std::cout<<sum_n1.scalar_as<arrow::Int64Scalar>().value<<std::endl;

    //calc sum element wise
    // create output
    arrow::Datum sum_elem_wise;
    // call generic callFunction
    ARROW_ASSIGN_OR_RAISE(sum_elem_wise, arrow::compute::CallFunction("add",{table->GetColumnByName("n1"),table->GetColumnByName("n2")}));
    // get results
    // lets just print the type to see which type
    std::cout<<"sum_elem_wise Datum is of kind'"
        <<sum_elem_wise.ToString()
        <<"' and type of content '"
        <<sum_elem_wise.type()->ToString()<<"'"
        <<std::endl;
    // get and print results as array/chunked array (note: no elem type needed)
    std::cout<<sum_elem_wise.chunked_array()->ToString()<<std::endl;

    //searching for a value
    // create res which shall store searching info about value "666" in n1
    arrow::Datum res;
    // create and configure compute configurations
    arrow::compute::IndexOptions index_options;
    index_options.value = arrow::MakeScalar(666);
    // search for "666" in column n1 via calling the generic compute function callFunction
    ARROW_ASSIGN_OR_RAISE(res,arrow::compute::CallFunction("index",{table->GetColumnByName("n1")},&index_options));
    
    // print res type s
    std::cout<<"res Datum is of kind'"
        <<sum_elem_wise.ToString()
        <<"' and type of content '"
        <<sum_elem_wise.type()->ToString()<<"'"
        <<std::endl;
    // print res content, which will be an zero based index into n1
    std::cout<<res.scalar_as<arrow::Int64Scalar>().value<<std::endl;

    // count distinct:
    ARROW_ASSIGN_OR_RAISE(res,arrow::compute::CallFunction("count_distinct",{table->GetColumnByName("n1")}));
    std::cout<<"amount of distinct numbers in n1:"<<res.scalar_as<arrow::Int64Scalar>().value<<std::endl;

    // min/max:
    arrow::compute::ScalarAggregateOptions compute_options;
    compute_options.skip_nulls = false;
    arrow::Datum compute_res;
    //  min_max computes both and saves them in an struct with two elements
    ARROW_ASSIGN_OR_RAISE(compute_res,arrow::compute::CallFunction("min_max",{table->GetColumnByName("n1")},&compute_options));
    S(arrow::Scalar,min_value) = compute_res.scalar_as<arrow::StructScalar>().value[0];
    S(arrow::Scalar,max_value) = compute_res.scalar_as<arrow::StructScalar>().value[1];


    // filter:  
    // - built-in "filter" or "array_filter" can filter elements of a row via array of boolean which might not be the inital idea 
    // - built-in "greater" or .. functions only opperate on a single datum
    /*arrow::compute::FilterOptions filter_options;
    ARROW_ASSIGN_OR_RAISE(res,arrow::compute::CallFunction("filter",{n1,bools},&filter_options));
    */
    
    // "select * from table where n2 > 1000":
    
    //  v1: use dataset scanner:
    //   - expression and scanner creation:
    S(arrow::dataset::InMemoryDataset,dataset) = std::make_shared<arrow::dataset::InMemoryDataset>(table);
    S(arrow::dataset::ScanOptions,scanOptions) = std::make_shared<arrow::dataset::ScanOptions>();
    scanOptions->filter = 
        arrow::compute::greater(arrow::compute::field_ref("n2"),arrow::compute::literal(1000));
    auto scanner = arrow::dataset::ScannerBuilder(dataset,scanOptions).Finish();
    //   - scanner execution:
    ARROW_ASSIGN_OR_RAISE(res,scanner.ValueUnsafe()->ToTable());
    auto filteredTable = res.table();
    std::cout << "filtered table via dataset: "<< std::endl << filteredTable->ToString() << std::endl;
    
    //  v2: use gandiva expression compiler:
    //   - expression creation:
    //   note: only works on record batches, not on tables
    S(arrow::RecordBatch,data_rb) =
        arrow::RecordBatch::Make(schema,n2->length(),{n1,n2});
    S(gandiva::Node,gNField_n2) = gandiva::TreeExprBuilder::MakeField(field_n2);
    S(gandiva::Node,gNLiteral) = gandiva::TreeExprBuilder::MakeLiteral(1000);
    S(gandiva::Node,gNCond) = 
        gandiva::TreeExprBuilder::MakeFunction("greater_than",{gNField_n2,gNLiteral},arrow::boolean());
    S(gandiva::Condition,gCond) =
        gandiva::TreeExprBuilder::MakeCondition(gNCond);
    //   - filter creation and execution:
    S(gandiva::Filter,f);
    ARROW_RETURN_NOT_OK(gandiva::Filter::Make(schema,gCond, &f));
    S(gandiva::SelectionVector, res_idxs);
    ARROW_RETURN_NOT_OK(gandiva::SelectionVector::MakeInt16(data_rb->num_rows(),
        arrow::default_memory_pool(),&res_idxs));
    ARROW_RETURN_NOT_OK(f->Evaluate(*data_rb,res_idxs));
    //   - filter result evaluation:
    S(arrow::Array,idxs) = res_idxs->ToArray();
    arrow::Datum r; ARROW_ASSIGN_OR_RAISE(r, 
        arrow::compute::Take(arrow::Datum(data_rb),arrow::Datum(idxs),arrow::compute::TakeOptions::NoBoundsCheck()));
    std::cout<<" filtered table via gandiva: "<<std::endl<<r.record_batch()->ToString()<<std::endl;
    
    return arrow::Status::OK();
}

arrow::Status GandivaStuff(){
    // create a column
    arrow::Int32Builder i32b;
    int32_t n2_raw[5] = {1,2,3,4,5};
    ARROW_RETURN_NOT_OK(i32b.AppendValues(n2_raw,5));
    ARROW_ASSIGN_OR_RAISE(S(arrow::Array,n2), i32b.Finish());
    S(arrow::Field,field_n2) = arrow::field("n2",arrow::int32());
    
    //  1. expr tree creation:
    S(gandiva::Node,gNField_n2) = gandiva::TreeExprBuilder::MakeField(field_n2);
    S(gandiva::Node,gNLiteral) = gandiva::TreeExprBuilder::MakeLiteral(2);
    S(arrow::Field,field_res)=arrow::field("result",arrow::int32());
    S(gandiva::Node,gNCond) = 
        gandiva::TreeExprBuilder::MakeFunction("greater_than",{gNField_n2,gNLiteral},arrow::boolean());
    S(gandiva::Node,gNAdd) = 
        gandiva::TreeExprBuilder::MakeFunction("add",{gNField_n2,gNLiteral},arrow::int32());
    //   transformation expr:
    S(gandiva::Expression,gNTrans) =
        gandiva::TreeExprBuilder::MakeExpression(gNAdd,field_res);
    //   selection expr:
    S(gandiva::Condition,gNSel) =
        gandiva::TreeExprBuilder::MakeCondition(gNCond);
    // retrieve available function names via gandiva::GetRegisteredFunctionSignatures()

    //  2. processors for nodes:
    //   projector: copies data 
    //   filter: saves data refs (as idx-vector)
    S(arrow::Schema,input_schema) = arrow::schema({field_n2});
    S(arrow::Schema,output_schema) = arrow::schema({field_res});
    
    S(gandiva::Projector,p);
    arrow::Status s;
    std::vector<std::shared_ptr<gandiva::Expression>> es = {gNTrans};
    s = gandiva::Projector::Make(input_schema,es,&p);
    ARROW_RETURN_NOT_OK(s);

    S(gandiva::Filter,f);
    s = gandiva::Filter::Make(input_schema,gNSel, &f);
    ARROW_RETURN_NOT_OK(s);
    //  3. executing projection / filtering
    S(arrow::RecordBatch,input) =
        arrow::RecordBatch::Make(input_schema,n2->length(),{n2});
    arrow::ArrayVector outs;
    s = p->Evaluate(*input,arrow::default_memory_pool(),&outs);
    ARROW_RETURN_NOT_OK(s);
    
    S(arrow::RecordBatch,outs_rb) =
        arrow::RecordBatch::Make(output_schema,outs[0]->length(),outs);
    
    std::cout<<" gandiva projection only returns: "<<std::endl<<outs_rb->ToString()<<std::endl;
    // = 3,4,5,6,7

    S(gandiva::SelectionVector, res_idxs);
    s = gandiva::SelectionVector::MakeInt16(input->num_rows(),arrow::default_memory_pool(),&res_idxs);
    ARROW_RETURN_NOT_OK(s);
    s = f->Evaluate(*input,res_idxs);
    ARROW_RETURN_NOT_OK(s);
    S(arrow::Array,idxs) = res_idxs->ToArray();
    //  use custom fct to retrieve elements from idxs 
    arrow::Datum bla; ARROW_ASSIGN_OR_RAISE(bla, 
        arrow::compute::Take(arrow::Datum(input),arrow::Datum(idxs),arrow::compute::TakeOptions::NoBoundsCheck()));
    outs_rb = bla.record_batch();
    
    std::cout<<" gandiva filtering only returns: "<<std::endl<<outs_rb->ToString()<<std::endl;
    // = 3,4,5

    //  4. chaining the executions (this should be used instead of 3 if sql smth like see below)
    //  select l.n1 + 2 from (select <c> as n1 from <table> where <c> > 2) l  
    //   configure projector to take filtered input
    s = gandiva::Projector::Make(input_schema,es,res_idxs->GetMode(),gandiva::ConfigurationBuilder::DefaultConfiguration(),&p);
    ARROW_RETURN_NOT_OK(s);
    s = p->Evaluate(*input,res_idxs.get(),arrow::default_memory_pool(),&outs);
    ARROW_RETURN_NOT_OK(s);
    outs_rb = arrow::RecordBatch::Make(output_schema,outs[0]->length(),outs);

    std::cout<<" gandiva filtering; projection; returns: "<<std::endl<<outs_rb->ToString()<<std::endl;
    // = 5,6,7
    
    return arrow::Status::OK();
}

// Generate some data for the rest of this example.
arrow::Result<std::shared_ptr<arrow::Table>> CreateTable() {
    // This code should look familiar from the basic Arrow example, and is not the
    // focus of this example. However, we need data to work on it, and this makes that!
    auto schema =
    arrow::schema({arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64()),
    arrow::field("c", arrow::int64())});
    S(arrow::Array,array_a);
    S(arrow::Array,array_b);
    S(arrow::Array,array_c);
    arrow::NumericBuilder<arrow::Int64Type> builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    ARROW_RETURN_NOT_OK(builder.Finish(&array_a));
    builder.Reset();
    ARROW_RETURN_NOT_OK(builder.AppendValues({9, 8, 7, 6, 5, 4, 3, 2, 1, 0}));
    ARROW_RETURN_NOT_OK(builder.Finish(&array_b));
    builder.Reset();
    ARROW_RETURN_NOT_OK(builder.AppendValues({1, 2, 1, 2, 1, 2, 1, 2, 1, 2}));
    ARROW_RETURN_NOT_OK(builder.Finish(&array_c));
    return arrow::Table::Make(schema, {array_a, array_b, array_c});
}

// Set up a dataset by writing two Parquet files.
arrow::Result<std::string> CreateExampleParquetDataset(
    const std::shared_ptr<arrow::fs::FileSystem>& filesystem,
    const std::string& root_path) {
    // Much like CreateTable(), this is utility that gets us the dataset we'll be reading
    // from. Don't worry, we also write a dataset in the example proper.
    auto base_path = root_path + "/" BUILDDIR "/parquet_dataset";
    ARROW_RETURN_NOT_OK(filesystem->CreateDir(base_path));
    // Create an Arrow Table
    ARROW_ASSIGN_OR_RAISE(auto table, CreateTable());
    // Write it into two Parquet files
    ARROW_ASSIGN_OR_RAISE(auto output,
    filesystem->OpenOutputStream(base_path + "/data1.parquet"));
    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
    *table->Slice(0, 5), arrow::default_memory_pool(), output, 2048));
    ARROW_ASSIGN_OR_RAISE(output,
    filesystem->OpenOutputStream(base_path + "/data2.parquet"));
    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
    *table->Slice(5), arrow::default_memory_pool(), output, 2048));
    return base_path;
}

arrow::Status PrepareEnv() {
    // Get our environment prepared for reading, by setting up some quick writing.
    ARROW_ASSIGN_OR_RAISE(auto src_table, CreateTable())
    // Note this operates in the directory the executable is built in.
    char setup_path[256];
    char* result = getcwd(setup_path, 256);
    if (result == NULL) {
    return arrow::Status::IOError("Fetching PWD failed.");
    }

    ARROW_ASSIGN_OR_RAISE(S(arrow::fs::FileSystem,setup_fs), arrow::fs::FileSystemFromUriOrPath(setup_path));
    ARROW_ASSIGN_OR_RAISE(auto dset_path, CreateExampleParquetDataset(setup_fs, "."));

    return arrow::Status::OK();
}

arrow::Status ReadAndWritePartitionedDatasets()
{
    //read fragmented data
    ARROW_RETURN_NOT_OK(PrepareEnv());
    // create os filesystem interface obj 
    S(arrow::fs::FileSystem,fs);
    // cd to this folder (using cwd via unistd)
    char init_path[256];
    char* result = getcwd(init_path,256);
    if(result == NULL)return arrow::Status::IOError("fetching cwd failed");
    ARROW_ASSIGN_OR_RAISE(fs, arrow::fs::FileSystemFromUriOrPath(init_path));
    // create and configure a selector
    arrow::fs::FileSelector selector;
    selector.base_dir = BUILDDIR "/parquet_dataset";
    selector.recursive = true;
    // create dataset factory options obj
    arrow::dataset::FileSystemFactoryOptions options;
    options.partitioning = arrow::dataset::HivePartitioning::MakeFactory();
    // create type obj of to be read format
    M(arrow::dataset::ParquetFileFormat,read_format,());
    // now create our factory which will read parque format files in ./parque_dataset and create datasets
    ARROW_ASSIGN_OR_RAISE(auto factory, arrow::dataset::FileSystemDatasetFactory::Make(fs,selector,read_format,options));
    // read all:
    ARROW_ASSIGN_OR_RAISE(auto read_dataset,factory->Finish());
    // retrieve fragments:
    ARROW_ASSIGN_OR_RAISE(auto fragments, read_dataset->GetFragments());
    // print fragments:
    for(const auto& fragment: fragments)
    {
        std::cout<<"found fragment: "<<(*fragment)->ToString() <<std::endl;
        std::cout<<"partition expression: "<<(*fragment)->partition_expression().ToString()<<std::endl;
    }
    // create a table from the dataset(s)
    // create a builder for a scanner which will create the table
    ARROW_ASSIGN_OR_RAISE(auto read_scanner_builder, read_dataset->NewScan());
    // build the scanner
    ARROW_ASSIGN_OR_RAISE(auto read_scanner, read_scanner_builder->Finish());
    // use the scanner to create a table
    ARROW_ASSIGN_OR_RAISE(S(arrow::Table,table), read_scanner->ToTable());
    // print the table!
    std::cout<<table->ToString()<<std::endl;
    
    //write a dataset (as multiple files)
    //or: partitioning an arrow table on disk
    //lets consider splitting up the just read table based on cell values of row "a"
    // create batch reader 
    M(arrow::TableBatchReader,write_datasets,(table));
    // create scanner for creating tables from given table via builder 
    auto write_scanner_builder = arrow::dataset::ScannerBuilder::FromRecordBatchReader(write_datasets);
    ARROW_ASSIGN_OR_RAISE(auto writer_scanner, write_scanner_builder->Finish());
    // configure split schema..
    auto partition_schema = arrow::schema({arrow::field("a", arrow::utf8())});
    // configure partitioning algorithm
    M(arrow::dataset::HivePartitioning,partitioning,(partition_schema));
    // configure output file format
    M(arrow::dataset::ParquetFileFormat,write_format,());
    // configure filesystem write options
    arrow::dataset::FileSystemDatasetWriteOptions write_options;
    write_options.file_write_options = write_format->DefaultWriteOptions();
    write_options.filesystem = fs; // cwd
    write_options.base_dir = BUILDDIR "/write_dataset"; //folder to be created within cwd
    write_options.partitioning = partitioning;
    write_options.basename_template = "part{i}.parquet"; //gen filenames template
    write_options.existing_data_behavior = arrow::dataset::ExistingDataBehavior::kOverwriteOrIgnore; //if files already ex, overwrite them
    // fragment the table and write files to disk:
    ARROW_RETURN_NOT_OK(arrow::dataset::FileSystemDataset::Write(write_options,writer_scanner));
    
    return arrow::Status::OK();
}

arrow::Status CustomCompute()
{
    //create a table with two columns each 5 rows
    arrow::Int32Builder i32b;
    int32_t n1_raw[5] = {42,24,666,111,9876};
    int32_t n2_raw[5] = {4,1999,2023,777,6};
    ARROW_RETURN_NOT_OK(i32b.AppendValues(n1_raw,5));
    ARROW_ASSIGN_OR_RAISE(S(arrow::Array,n1), i32b.Finish());
    ARROW_RETURN_NOT_OK(i32b.AppendValues(n2_raw,5));
    ARROW_ASSIGN_OR_RAISE(S(arrow::Array,n2), i32b.Finish());
    S(arrow::Field,field_n1) = arrow::field("n1",arrow::int32());
    S(arrow::Field,field_n2) = arrow::field("n2",arrow::int32());
    S(arrow::Schema,schema) = arrow::schema({field_n1,field_n2});
    S(arrow::Table,table) = arrow::Table::Make(schema,{n1,n2},5);

    //custom compute functions:
    // 1. functions are registered at the arrow::compute::FunctionRegistry
    // 2. global FunctionRegistry (which will be used when calling fct CallFunction) can be retrieved by calling arrow::compute::GetFunctionRegistry (its also possible to create local fctregistries and use them(however not via CallFunction))
    // 3. FunctionRegistry has a AddFunction() which registers arrow::compute::Function
    // 4. Function has subclass arrow::compute::FunctionImpl,.., arrow::compute::ScalarFunction,arrow::compute::ScalarAggregateFunction,..
    // 5. add implementation to Function subclasses via AddKernel(..) with a corresponding Kernel, e.g. ScalarFunction needs ScalarKernel 
    // 6. Kernel subclasses are type specific function implementations, pass in fct ptr to own behavior on construction
    // 7. now call CallFunction as usually
    // update: the registries do not have methods for removing functions, so heap allocated function objects are required
    //  for testing purpose, using a local functionregistry

    // e.g. building the built-in functions for (element wise) add and aggregate add 
    //  only use them inside this fct
    //  declare common vars:
    arrow::compute::InputType input_type(arrow::int32());
    S(arrow::compute::FunctionExecutor,exec);
    U(arrow::compute::FunctionRegistry,funcReg) = arrow::compute::FunctionRegistry::Make(arrow::compute::GetFunctionRegistry());
    S(arrow::ChunkedArray,_n1) = table->GetColumnByName("n1");
    S(arrow::ChunkedArray,_n2) = table->GetColumnByName("n2");
    arrow::Datum call_res;
    //  1. element wise add:
    //   ScalarKernel takes a fct ptr, fct will be called once containing the columns and expecting the fct to write values back to given param,
    //   ScalarKernel creates result column of same size as given columns so fct just needs to write values  
    //   - register stuff:
    M(arrow::compute::ScalarFunction,add_elem_doc,(
        "add_elemwise",
        arrow::compute::Arity::Binary() , 
        arrow::compute::FunctionDoc("add elementwise","custom function simulating add",{"o1","o2"})));
    arrow::compute::ScalarKernel addElem({input_type,input_type},
        arrow::compute::OutputType(arrow::int32()),&add1);
    ARROW_RETURN_NOT_OK(add_elem_doc->AddKernel(addElem));
    ARROW_RETURN_NOT_OK(funcReg->AddFunction(add_elem_doc));
    //   - call stuff:
    ARROW_ASSIGN_OR_RAISE(exec, arrow::compute::GetFunctionExecutor("add_elemwise",{_n1,_n2},NULLPTR,funcReg.get()));
    ARROW_ASSIGN_OR_RAISE(call_res,exec->Execute({_n1,_n2}));
    std::cout<<"res add_elemwise Datum is of kind '"
        <<call_res.ToString()
        <<"' and type of content '"
        <<call_res.type()->ToString()<<"'"
        <<std::endl;
    std::cout<<"res custom elem wise add:"<<call_res.chunked_array()->ToString()<<std::endl;
    
    arrow::Datum call_res_;
    
    ARROW_ASSIGN_OR_RAISE(call_res_, arrow::compute::CallFunction("add",{_n1,_n2}));
    std::cout<<"res built-in elem wise add:"<<call_res_.chunked_array()->ToString()<<std::endl;
    //  2. aggregate add:
    //   ScalarAggregateKernel takes 4 fct ptr: init, consume, merge, finalize
    //   fcts called once in the given order
    //   init creates arrow::compute::KernelState and will be assigned to param ctx 
    //   KernelState manages results of the computation
    //   consume executes computation given columns storing results in the state
    //   finalize writes results back to Datum param
    //    init: KernelInit =  std::function<Result<std::unique_ptr<KernelState>>(KernelContext*, const KernelInitArgs&)>;
    //    consume: ScalarAggregateConsume = Status (*)(KernelContext*, const ExecSpan&);
    //    merge: ScalarAggregateMerge = Status (*)(KernelContext*, KernelState&&, KernelState*);
    //    finalize: ScalarAggregateFinalize = Status (*)(KernelContext*, Datum*);
    //   - register stuff:
    M(arrow::compute::ScalarAggregateFunction,add_agg_doc,(
        "add_agg",
        arrow::compute::Arity::Unary(),
        arrow::compute::FunctionDoc("add aggregate","custom function simulating aggregate add",{"column"})));
    arrow::compute::ScalarAggregateKernel addAgg({input_type},
        arrow::compute::OutputType(arrow::int64()),
        initK<CustomSumKernelState>,
        consumeK<CustomSumKernelState>,
        mergeK<CustomSumKernelState>,
        finalizeK<CustomSumKernelState>,false);
    ARROW_RETURN_NOT_OK(add_agg_doc->AddKernel(addAgg));
    ARROW_RETURN_NOT_OK(funcReg->AddFunction(add_agg_doc));
    //   - call stuff:
    ARROW_ASSIGN_OR_RAISE(exec, arrow::compute::GetFunctionExecutor("add_agg",{_n1},NULLPTR,funcReg.get()));
    ARROW_ASSIGN_OR_RAISE(call_res,exec->Execute({_n1}));
    //ARROW_ASSIGN_OR_RAISE(call_res,arrow::compute::CallFunction("add_agg",{n1}));
    std::cout<<"sum of custom column n1 Datum is of kind'"
        <<call_res.ToString()
        <<"' and type of content '"
        <<call_res.type()->ToString()<<"'"
        <<std::endl;
    std::cout<<"custom sum n1:"<<call_res.scalar_as<arrow::Int64Scalar>().value<<std::endl;
    ARROW_ASSIGN_OR_RAISE(exec, arrow::compute::GetFunctionExecutor("add_agg",{_n2},NULLPTR,funcReg.get()));
    ARROW_ASSIGN_OR_RAISE(call_res,exec->Execute({_n2}));
    //ARROW_ASSIGN_OR_RAISE(call_res,arrow::compute::CallFunction("add_agg",{n2}));
    std::cout<<"custom sum n2:"<<call_res.scalar_as<arrow::Int64Scalar>().value<<std::endl;
    //   - compare with built in agg sum fct results:
    ARROW_ASSIGN_OR_RAISE(call_res,arrow::compute::CallFunction("sum",{_n1}));
    std::cout<<"built-in sum n1:"<<call_res.scalar_as<arrow::Int64Scalar>().value<<std::endl;
    ARROW_ASSIGN_OR_RAISE(call_res,arrow::compute::CallFunction("sum",{_n2}));
    std::cout<<"built-in sum n2:"<<call_res.scalar_as<arrow::Int64Scalar>().value<<std::endl;
    
    return arrow::Status::OK();
}

arrow::Status RunMain()
{
    //tutorial stuff
    ARROW_RETURN_NOT_OK(ReadAndWriteStuff());
    ARROW_RETURN_NOT_OK(ComputeStuff());
    ARROW_RETURN_NOT_OK(GandivaStuff());
    ARROW_RETURN_NOT_OK(ReadAndWritePartitionedDatasets());
    //custom compute stuff
    ARROW_RETURN_NOT_OK(CustomCompute());
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