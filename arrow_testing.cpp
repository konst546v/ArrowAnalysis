#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <arrow/compute/api.h>

#include <unistd.h>
#include <iostream>


arrow::Status GenInitialFile(){
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
    
    
    //create chunkedarray
    // create origin arrays using the already ex. builders
    int8_t days_raw2[5] = {1,2,3,4,5};
    ARROW_RETURN_NOT_OK(int8builder.AppendValues(days_raw2,5));
    std::shared_ptr<arrow::Array> days2;
    ARROW_ASSIGN_OR_RAISE(days2, int8builder.Finish());

    int8_t months_raw2[5] = {2,2,1,4,1};
    ARROW_RETURN_NOT_OK(int8builder.AppendValues(months_raw2,5));
    std::shared_ptr<arrow::Array> months2;
    ARROW_ASSIGN_OR_RAISE(months2, int8builder.Finish());

    int16_t years_raw2[5] = {1999,2000,2001,2003,2004};
    ARROW_RETURN_NOT_OK(int16builder.AppendValues(years_raw2,5));
    std::shared_ptr<arrow::Array> years2;
    ARROW_ASSIGN_OR_RAISE(years2,int16builder.Finish());
    //create "placeholder" containers
    arrow::ArrayVector day_vecs{days,days2}; //internal array of array container 
    
    //create chunked array
    // - understandable as some kind of cheap concatenated array 
    std::shared_ptr<arrow::ChunkedArray> day_chunks = std::make_shared<arrow::ChunkedArray>(day_vecs);

    // Repeat for months.
    arrow::ArrayVector month_vecs{months, months2};
    std::shared_ptr<arrow::ChunkedArray> month_chunks =
    std::make_shared<arrow::ChunkedArray>(month_vecs);

    // Repeat for years.
    arrow::ArrayVector year_vecs{years, years2};
    std::shared_ptr<arrow::ChunkedArray> year_chunks =
    std::make_shared<arrow::ChunkedArray>(year_vecs);

    //create tables:
    // - recordbatch created from array
    // - table created from chunkedarray
    // - both use same schema for describing columns
    // -> recordbatch has limited rowsize since arraysize limited
    // -> table can be bigger but doesnt guarantee columns in columnar format
    std::shared_ptr<arrow::Table> table;
    table = arrow::Table::Make(schema,{day_chunks,month_chunks,year_chunks});

    
    // Write out test files in IPC, CSV, and Parquet for the example to use.
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_in.arrow"));
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ipc::RecordBatchWriter> ipc_writer,arrow::ipc::MakeFileWriter(outfile, schema));
    ARROW_RETURN_NOT_OK(ipc_writer->WriteTable(*table));
    ARROW_RETURN_NOT_OK(ipc_writer->Close());

    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_in.csv"));
    ARROW_ASSIGN_OR_RAISE(auto csv_writer,arrow::csv::MakeCSVWriter(outfile, table->schema()));
    ARROW_RETURN_NOT_OK(csv_writer->WriteTable(*table));
    ARROW_RETURN_NOT_OK(csv_writer->Close());

    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_in.parquet"));
    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, 5));


    return arrow::Status::OK();
}

arrow::Status ReadAndWriteStuff()
{
    ARROW_RETURN_NOT_OK(GenInitialFile());
    
    //read a (ipc) file
    // create an object which acts as some kind of container for the opened file
    std::shared_ptr<arrow::io::ReadableFile> inFile;
    // open the file
    ARROW_ASSIGN_OR_RAISE(inFile, arrow::io::ReadableFile::Open("test_in.arrow",arrow::default_memory_pool()));
    // use a more concrete reader to read from the container e.g. assume the contents to be recordbatches (tables)
    std::shared_ptr<arrow::ipc::RecordBatchFileReader> ipc_reader;
    ARROW_ASSIGN_OR_RAISE(ipc_reader,arrow::ipc::RecordBatchFileReader::Open(inFile));
    // get contents into a recordbatch
    std::shared_ptr<arrow::RecordBatch> recordbatch;
    ARROW_ASSIGN_OR_RAISE(recordbatch, ipc_reader->ReadRecordBatch(0)); //there can be many recordbatches in a file
    //write a (ipc) file
    std::shared_ptr<arrow::io::FileOutputStream> outFile;
    // bind the object to the file to be written to
    ARROW_ASSIGN_OR_RAISE(outFile, arrow::io::FileOutputStream::Open("test_out.arrow"));
    // create an object responsible for writing spec arrow ds
    // in this case, use a recordbatch because we want to write the just read recordbatch
    // in order to create a recordbatch writer we need the output file obj and the schema,
    //  use just created obj file and schema from read recordbatch
    std::shared_ptr<arrow::ipc::RecordBatchWriter> ipc_writer;
    ARROW_ASSIGN_OR_RAISE(ipc_writer,arrow::ipc::MakeFileWriter(outFile,recordbatch->schema()));
    // write contents to physical file:
    ARROW_RETURN_NOT_OK(ipc_writer->WriteRecordBatch(*recordbatch));
    // close file (ipc writers special use case)
    ARROW_RETURN_NOT_OK(ipc_writer->Close());

    //read a (csv) file
    //reuse input and output file objs
    ARROW_ASSIGN_OR_RAISE(inFile, arrow::io::ReadableFile::Open("test_in.csv"));
    // create table ptr of contents to be read to
    std::shared_ptr<arrow::Table> csv_table;
    // create a csv reader with default options
    std::shared_ptr<arrow::csv::TableReader> csv_reader;
    ARROW_ASSIGN_OR_RAISE(csv_reader, arrow::csv::TableReader::Make(
        arrow::io::default_io_context(),
        inFile,
        arrow::csv::ReadOptions::Defaults(),
        arrow::csv::ParseOptions::Defaults(),
        arrow::csv::ConvertOptions::Defaults()));
    // read actual contents
    ARROW_ASSIGN_OR_RAISE(csv_table, csv_reader->Read());
    //write a (csv) file
    // get output file obj, use already ex obj
    ARROW_ASSIGN_OR_RAISE(outFile,arrow::io::FileOutputStream::Open("test_out.csv"));
    // create writer, dont ask why its now also the ipc recordbatch writer
    std::shared_ptr<arrow::ipc::RecordBatchWriter> csv_writer;
    ARROW_ASSIGN_OR_RAISE(csv_writer,arrow::csv::MakeCSVWriter(outFile,csv_table->schema()));
    // write content
    ARROW_RETURN_NOT_OK(csv_writer->WriteTable(*csv_table));
    // close file
    ARROW_RETURN_NOT_OK(csv_writer->Close());

    //read a (parquet) file
    // get input file
    ARROW_ASSIGN_OR_RAISE(inFile, arrow::io::ReadableFile::Open("test_in.parquet"));
    // get parquet reader (note: extern dependency)
    std::unique_ptr<parquet::arrow::FileReader> pReader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(inFile,arrow::default_memory_pool(), &pReader));
    // create table ptr and read contents into arrow table using the parquet reader
    std::shared_ptr<arrow::Table> pTable;
    PARQUET_THROW_NOT_OK(pReader->ReadTable(&pTable));
    //writing a (parquet) file
    // get output file
    ARROW_ASSIGN_OR_RAISE(outFile, arrow::io::FileOutputStream::Open("test_out.parquet"));
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
    std::shared_ptr<arrow::Array> n1,n2;
    ARROW_RETURN_NOT_OK(i32b.AppendValues(n1_raw,5));
    ARROW_ASSIGN_OR_RAISE(n1, i32b.Finish());
    ARROW_RETURN_NOT_OK(i32b.AppendValues(n2_raw,5));
    ARROW_ASSIGN_OR_RAISE(n2, i32b.Finish());
    std::shared_ptr<arrow::Field> field_n1,field_n2;
    std::shared_ptr<arrow::Schema> schema;
    field_n1 = arrow::field("n1",arrow::int32());
    field_n2 = arrow::field("n2",arrow::int32());
    schema = arrow::schema({field_n1,field_n2});
    std::shared_ptr<arrow::Table> table;
    table = arrow::Table::Make(schema,{n1,n2},5);

    //calc sum over an array
    // create result obj
    // very generic object holding every type of result
    arrow::Datum sum_n1;
    // call arrow conv func
    ARROW_ASSIGN_OR_RAISE(sum_n1,arrow::compute::Sum({table->GetColumnByName("n1")}));
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
    // get results
    // print res type s
    std::cout<<"res Datum is of kind'"
        <<sum_elem_wise.ToString()
        <<"' and type of content '"
        <<sum_elem_wise.type()->ToString()<<"'"
        <<std::endl;
    // print res content, which will be an zero based index into n1
    std::cout<<res.scalar_as<arrow::Int64Scalar>().value<<std::endl;

    return arrow::Status::OK();
}

// Generate some data for the rest of this example.
arrow::Result<std::shared_ptr<arrow::Table>> CreateTable() {
    // This code should look familiar from the basic Arrow example, and is not the
    // focus of this example. However, we need data to work on it, and this makes that!
    auto schema =
    arrow::schema({arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64()),
    arrow::field("c", arrow::int64())});
    std::shared_ptr<arrow::Array> array_a;
    std::shared_ptr<arrow::Array> array_b;
    std::shared_ptr<arrow::Array> array_c;
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
    auto base_path = root_path + "parquet_dataset";
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
    std::shared_ptr<arrow::fs::FileSystem> setup_fs;
    // Note this operates in the directory the executable is built in.
    char setup_path[256];
    char* result = getcwd(setup_path, 256);
    if (result == NULL) {
    return arrow::Status::IOError("Fetching PWD failed.");
    }

    ARROW_ASSIGN_OR_RAISE(setup_fs, arrow::fs::FileSystemFromUriOrPath(setup_path));
    ARROW_ASSIGN_OR_RAISE(auto dset_path, CreateExampleParquetDataset(setup_fs, ""));

    return arrow::Status::OK();
}

arrow::Status ReadAndWritePartitionedDatasets()
{
    //read fragmented data
    ARROW_RETURN_NOT_OK(PrepareEnv());
    // create os filesystem interface obj 
    std::shared_ptr<arrow::fs::FileSystem> fs;
    // cd to this folder (using cwd via unistd)
    char init_path[256];
    char* result = getcwd(init_path,256);
    if(result == NULL)return arrow::Status::IOError("fetching cwd failed");
    ARROW_ASSIGN_OR_RAISE(fs, arrow::fs::FileSystemFromUriOrPath(init_path));
    // create and configure a selector
    arrow::fs::FileSelector selector;
    selector.base_dir = "parquet_dataset";
    selector.recursive = true;
    // create dataset factory options obj
    arrow::dataset::FileSystemFactoryOptions options;
    options.partitioning = arrow::dataset::HivePartitioning::MakeFactory();
    // create type obj of to be read format
    auto read_format = std::make_shared<arrow::dataset::ParquetFileFormat>();
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
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> table, read_scanner->ToTable());
    // print the table!
    std::cout<<table->ToString()<<std::endl;
    
    //write a dataset (as multiple files)
    //or: partitioning an arrow table on disk
    //lets consider splitting up the just read table based on cell values of row "a"
    // create batch reader 
    std::shared_ptr<arrow::TableBatchReader> write_datasets = std::make_shared<arrow::TableBatchReader>(table);
    // create scanner for creating tables from given table via builder 
    auto write_scanner_builder = arrow::dataset::ScannerBuilder::FromRecordBatchReader(write_datasets);
    ARROW_ASSIGN_OR_RAISE(auto writer_scanner, write_scanner_builder->Finish());
    // configure split schema..
    auto partition_schema = arrow::schema({arrow::field("a", arrow::utf8())});
    // configure partitioning algorithm
    auto partitioning = std::make_shared<arrow::dataset::HivePartitioning>(partition_schema);
    // configure output file format
    auto write_format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    // configure filesystem write options
    arrow::dataset::FileSystemDatasetWriteOptions write_options;
    write_options.file_write_options = write_format->DefaultWriteOptions();
    write_options.filesystem = fs; // cwd
    write_options.base_dir = "write_dataset"; //folder to be created within cwd
    write_options.partitioning = partitioning;
    write_options.basename_template = "part{i}.parquet"; //gen filenames template
    write_options.existing_data_behavior = arrow::dataset::ExistingDataBehavior::kOverwriteOrIgnore; //if files already ex, overwrite them
    // fragment the table and write files to disk:
    ARROW_RETURN_NOT_OK(arrow::dataset::FileSystemDataset::Write(write_options,writer_scanner));
    
    return arrow::Status::OK();
}


arrow::Status RunMain()
{
    ARROW_RETURN_NOT_OK(GenInitialFile());
    ARROW_RETURN_NOT_OK(ReadAndWriteStuff());
    ARROW_RETURN_NOT_OK(ComputeStuff());
    ARROW_RETURN_NOT_OK(ReadAndWritePartitionedDatasets());

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