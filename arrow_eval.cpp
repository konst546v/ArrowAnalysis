#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/compute/api.h>

#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <chrono>
#include <map>

#include "arrow_custom_kernel.cpp"

//size in kilobytes
arrow::Status someTesting(int size,int measures)
{
    //1. create random data
    //2. save data in feather format
    //3. read data from file
    //4. register custom fcts
    //5. exec custom agg sum fct 
    //6. exec builtin agg sum fct 
    //7. exec custom elem wise sum fct 
    //8. exec builtin elem wise sum fct
    //9. create json file of measurements "measurements_<size>_<measures>.json"

    // also measuring time for all tasks
    std::chrono::high_resolution_clock::time_point start; 
    //func for retrieving execution time from start start
    auto getTime = [&start](){
        auto diff = std::chrono::high_resolution_clock::now() - start;
        //10⁹ ns = 1s
        return std::chrono::duration_cast<std::chrono::nanoseconds>(diff).count();
    };

    std::cout<<"eval for size:"<<size<<" and measures:"<<measures<<std::endl;
    // 1.:
    start = std::chrono::high_resolution_clock::now();
    const int maxValue = 1000;
    srand(std::chrono::duration_cast<std::chrono::nanoseconds>(start.time_since_epoch()).count());//init rand.
    arrow::Int32Builder i32B;
    //nrOfKilobyte * einzKilobyte / bytePerElement
    for(int i=0; i < size*1024/4; i++){
        int v = rand() % maxValue;
        if(v){
            ARROW_RETURN_NOT_OK(i32B.Append(v));
        }else{
            ARROW_RETURN_NOT_OK(i32B.AppendNull());
        }
    }
    ARROW_ASSIGN_OR_RAISE(S(arrow::Array,nums),i32B.Finish());

    S(arrow::Field,field_nums) = arrow::field("numbers",arrow::int32());
    S(arrow::Schema,schema) = arrow::schema({field_nums});
    S(arrow::Table,table) = arrow::Table::Make(schema,{nums});
    std::cout<<"data generation took:"<<getTime()<<"ns"<<std::endl;
    
    // 2.:
    start = std::chrono::high_resolution_clock::now();
    ARROW_ASSIGN_OR_RAISE(S(arrow::io::FileOutputStream,outfile),
        arrow::io::FileOutputStream::Open("./" BUILDDIR "/nums.arrow"));
    ARROW_ASSIGN_OR_RAISE(S(arrow::ipc::RecordBatchWriter,ipc_writer),
        arrow::ipc::MakeFileWriter(outfile, schema));
    ARROW_RETURN_NOT_OK(ipc_writer->WriteTable(*table));
    ARROW_RETURN_NOT_OK(ipc_writer->Close());
    std::cout<<"writing table took:"<<getTime()<<"ns"<<std::endl;

    // 3.:
    start = std::chrono::high_resolution_clock::now();
    ARROW_ASSIGN_OR_RAISE(S(arrow::io::ReadableFile,inFile),
        arrow::io::ReadableFile::Open("./" BUILDDIR "/nums.arrow",arrow::default_memory_pool()));
    
    ARROW_ASSIGN_OR_RAISE(S(arrow::ipc::RecordBatchFileReader,ipc_reader),
        arrow::ipc::RecordBatchFileReader::Open(inFile));
    S(arrow::RecordBatch,recordbatch);
    std::vector<std::shared_ptr<arrow::RecordBatch>> recordbatches;
    for(int i=0; i< ipc_reader->num_record_batches();i++){
        ARROW_ASSIGN_OR_RAISE(recordbatch, ipc_reader->ReadRecordBatch(i));
        recordbatches.push_back(recordbatch);
    }
    ARROW_ASSIGN_OR_RAISE(S(arrow::Table,table2),
        arrow::Table::FromRecordBatches(recordbatches));
    std::cout<<"reading table took:"<<getTime()<<"ns"<<std::endl;

    // 4.:
    start = std::chrono::high_resolution_clock::now();
    // - common:
    arrow::compute::InputType input_type(arrow::int32());
    U(arrow::compute::FunctionRegistry,funcReg) = 
        arrow::compute::FunctionRegistry::Make(arrow::compute::GetFunctionRegistry());
    // - custom:
    M(arrow::compute::ScalarAggregateFunction,add_agg_doc,(
        "add_agg",
        arrow::compute::Arity::Unary(),
        arrow::compute::FunctionDoc("add agg custom","custom function simulation aggregate add",{"column"})));
    arrow::compute::ScalarAggregateKernel addAgg({input_type},
        arrow::compute::OutputType(arrow::int64()),
        initK<CustomSumKernelState>,
        consumeK<CustomSumKernelState>,
        mergeK<CustomSumKernelState>,
        finalizeK<CustomSumKernelState>,false);
    ARROW_RETURN_NOT_OK(add_agg_doc->AddKernel(addAgg));
    ARROW_RETURN_NOT_OK(funcReg->AddFunction(add_agg_doc));
    // - elemwise custom:
    M(arrow::compute::ScalarFunction,add_elem_doc,(
        "add_elemwise",
        arrow::compute::Arity::Binary() , 
        arrow::compute::FunctionDoc("add elementwise custom","custom function simulating elemwise add",{"o1","o2"})));
    arrow::compute::ScalarKernel addElem({input_type,input_type},arrow::compute::OutputType(arrow::int32()),&add1);
    ARROW_RETURN_NOT_OK(add_elem_doc->AddKernel(addElem));
    ARROW_RETURN_NOT_OK(funcReg->AddFunction(add_elem_doc));

    std::cout<<"custom functions init took:"<<getTime()<<"ns"<<std::endl;
    
    // - calling common:
    arrow::Datum call_res;
    S(arrow::compute::FunctionExecutor,e);
    S(arrow::ChunkedArray,column) = table2->GetColumnByName("numbers");
    // save measurements
    //  naming convention: <func><type>
    std::map<std::string, std::vector<int64_t>> ms;
    std::vector<arrow::Datum> funcargs;
    auto measure = [&](std::string name, std::string funcname) {
        std::vector<int64_t>* m;
        ms.insert(std::make_pair<std::string,std::vector<int64_t>>(name.c_str(),{})); //aggregate sum custom
        m = &ms[name];//ref
        S(arrow::compute::FunctionExecutor,e);
        for(int i=0; i< measures; i++){
            //using the macro causes recursion
            start = std::chrono::high_resolution_clock::now();
            auto res = arrow::compute::GetFunctionExecutor(
                funcname,funcargs,NULLPTR,funcReg.get());
            if(!res.ok()){
                return arrow::Status::Cancelled("function exec couldnt be created");
            }
            e = res.ValueOrDie();
            
            auto call_res = e->Execute(funcargs);
            m->push_back(getTime());
            if(!call_res.ok()){
                return arrow::Status::Cancelled("function exec couldnt be exec");
            }
            std::cout<<name<<" res: "<<call_res.ValueOrDie().ToString()<<std::endl;
            std::cout<<name<<" exec time: "<<m->back()<<"ns"<<std::endl;
        }
        return arrow::Status::OK();
    };
    // 5.:
    funcargs = {column};
    ARROW_RETURN_NOT_OK(measure("asc","add_agg"));
    // 6.:
    ARROW_RETURN_NOT_OK(measure("asb","sum"));
    // 7.:
    funcargs = {column,column};
    ARROW_RETURN_NOT_OK(measure("esc","add_elemwise"));
    // 8.:
    ARROW_RETURN_NOT_OK(measure("esb","add"));


    // 9.:
    // one json file containing array of obj 
    std::stringstream ss;
    ss<<"./" BUILDDIR "/measurements_"<<size<<"_"<<measures<<".json";
    std::ofstream fileOut(ss.str());
    if(fileOut.is_open())
    {
        fileOut<<"["<<std::endl;
        for(int i = 0; i<measures;i++)
        {
            fileOut<<"{";
            for(auto it = ms.begin(); it != ms.end(); it++)
            {
                if(it != ms.begin())
                {
                    fileOut<<",";
                }
                fileOut<<'"'<<it->first<<"\":\""<<it->second.at(i)<<'"';
            }
            fileOut<<"}"<<((i!=measures-1)?",":"")<<std::endl;
        }
        fileOut<<"]";
        fileOut.close();
        std::cout<<"saved measurements"<<std::endl;
    }else 
    {
        std::cerr<<"wasnt able to save measurements"<<std::endl;
    }

    return arrow::Status::OK();
}


int main(int argc,char* argv[]){
    if(argc < 3)
    {
        std::cerr<<"missing arg(s), usage e.g.:"<<std::endl<<
            "./arrow_eval 16 50"<<std::endl<<
            " - for 16KB size and 50 measurements"<<std::endl;
        return 1;
    }
    arrow::Status st = someTesting(atoi(argv[1]),atoi(argv[2]));
    if(!st.ok()){
        std::cerr<<st<<std::endl;
        return 1;
    }
    return 0;
}