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
    //2. save data in ipc (binary) format
    //3. read data from file
    //4. register custom fcts
    //5. exec custom agg sum fct 
    //6. exec opt. custom fct
    //7. exec builtin agg sum fct 
    //8. exec custom elem wise sum fct 
    //9. exec builtin elem wise sum fct
    //10. create json file of measurements "measurements_<size>_<measures>.json"

    // also measuring time for all tasks
    std::chrono::high_resolution_clock::time_point start; 
    //func for retrtrieving execution time from start start
    auto getTime = [&start](){
        auto diff = std::chrono::high_resolution_clock::now() - start;
        //10⁹ ns = 1s
        return std::chrono::duration_cast<std::chrono::nanoseconds>(diff).count();
    };

    std::cout<<"eval for size:"<<size<<" and measures:"<<measures<<std::endl;
    // 1.:
    start = std::chrono::high_resolution_clock::now();
    const int maxValue = 1000;
    srand(std::chrono::duration_cast<std::chrono::nanoseconds>(start.time_since_epoch()).count());
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

    S(arrow::Field,field_nums) = arrow::field("numbers",arrow::int32()); //create(copy) field via ns function
    S(arrow::Schema,schema) = arrow::schema({field_nums}); //a date(rec) schema, or: a table entry
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
        ARROW_ASSIGN_OR_RAISE(recordbatch, ipc_reader->ReadRecordBatch(0)); //there can be many 
        recordbatches.push_back(recordbatch);
    }
    ARROW_ASSIGN_OR_RAISE(S(arrow::Table,table2),
        arrow::Table::FromRecordBatches(recordbatches));
    std::cout<<"reading table took:"<<getTime()<<"ns"<<std::endl;

    // 4.:
    start = std::chrono::high_resolution_clock::now();
    // - common:
    arrow::compute::InputType input_type(arrow::int32());
    arrow::compute::OutputType output_type(arrow::int32());
    U(arrow::compute::FunctionRegistry,funcReg) = arrow::compute::FunctionRegistry::Make(arrow::compute::GetFunctionRegistry());
    // - unopt.:
    M(arrow::compute::ScalarAggregateFunction,add_agg_doc,(
        "add_agg",
        arrow::compute::Arity::Unary(),
        arrow::compute::FunctionDoc("sum custom","custom function simulation aggregate add, unoptimized",{"column"})));
    arrow::compute::ScalarAggregateKernel addAgg({input_type},
        output_type,
        initK<CustomSumKernelState>,
        consumeK<CustomSumKernelState>,
        mergeK<CustomSumKernelState>,
        finalizeK<CustomSumKernelState>,false);
    ARROW_RETURN_NOT_OK(add_agg_doc->AddKernel(addAgg));
    ARROW_RETURN_NOT_OK(funcReg->AddFunction(add_agg_doc));
    // - opt.:
    M(arrow::compute::ScalarAggregateFunction,add_agg_o_doc,(
        "add_agg_o",
        arrow::compute::Arity::Unary(),
        arrow::compute::FunctionDoc("add aggregate optimized","custom function simulation aggregate add",{"column"})));
    arrow::compute::ScalarAggregateKernel addAggO({input_type},
        output_type,
        initK<OCustomSumKernelState>,
        consumeK<OCustomSumKernelState>,
        mergeK<OCustomSumKernelState>,
        finalizeK<OCustomSumKernelState>,false);
    ARROW_RETURN_NOT_OK(add_agg_o_doc->AddKernel(addAggO));
    ARROW_RETURN_NOT_OK(funcReg->AddFunction(add_agg_o_doc));
    // - elemwise custom:
    M(arrow::compute::ScalarFunction,add_elem_doc,(
        "add_elemwise",
        arrow::compute::Arity::Binary() , 
        arrow::compute::FunctionDoc("add elementwise","custom function simulating add",{"o1","o2"})));
    arrow::compute::ScalarKernel addElem({input_type,input_type},output_type,&add1);
    ARROW_RETURN_NOT_OK(add_elem_doc->AddKernel(addElem));
    ARROW_RETURN_NOT_OK(funcReg->AddFunction(add_elem_doc));

    std::cout<<"custom functions init took:"<<getTime()<<"ns"<<std::endl;
    
    // - calling common:
    arrow::Datum call_res;
    S(arrow::compute::FunctionExecutor,e);
    // save measurements
    std::map<std::string, std::vector<int64_t>> ms;
    std::vector<int64_t>* m;

    // 5.: 
    ms.insert(std::make_pair<std::string,std::vector<int64_t>>("asc",{}));
    m = &ms["asc"];//ref
    for(int i=0; i< measures; i++){
        ARROW_ASSIGN_OR_RAISE(e,
            arrow::compute::GetFunctionExecutor("add_agg",{table2->GetColumnByName("numbers")},NULLPTR,funcReg.get()));
        start = std::chrono::high_resolution_clock::now();
        ARROW_ASSIGN_OR_RAISE(call_res,
            e->Execute({table2->GetColumnByName("numbers")}));
        m->push_back(getTime());
        std::cout<<"custom sum res: "<<call_res.scalar_as<arrow::Int64Scalar>().value<<std::endl;
        std::cout<<"custom sum exec time: "<<m->back()<<"ns"<<std::endl;
    }
    // 6.:
    ms.insert(std::make_pair<std::string,std::vector<int64_t>>("aso",{}));
    m = &ms["aso"];
    for(int i=0; i< measures; i++){
        ARROW_ASSIGN_OR_RAISE(e,
            arrow::compute::GetFunctionExecutor("add_agg_o",{table2->GetColumnByName("numbers")},NULLPTR,funcReg.get()));
        start = std::chrono::high_resolution_clock::now();
        ARROW_ASSIGN_OR_RAISE(call_res,
            e->Execute({table2->GetColumnByName("numbers")}));
        m->push_back(getTime());
        std::cout<<"opt. custom sum res: "<<call_res.scalar_as<arrow::Int64Scalar>().value<<std::endl;
        std::cout<<"opt. custom sum exec time: "<<m->back()<<"ns"<<std::endl;
    }
    // 7.:
    ms.insert(std::make_pair<std::string,std::vector<int64_t>>("asb",{}));
    m = &ms["asb"];
    std::cout<<"start measuring"<<std::endl;
    for(int i=0; i< measures; i++){
        start = std::chrono::high_resolution_clock::now();
        ARROW_ASSIGN_OR_RAISE(call_res,arrow::compute::CallFunction("sum",{table2->GetColumnByName("numbers")}));
        m->push_back(getTime());
        std::cout<<"built-in sum res: "<<call_res.scalar_as<arrow::Int64Scalar>().value<<std::endl;
        std::cout<<"built-in sum exec time: "<<m->back()<<"ns"<<std::endl;
    }
    // 8.:
    ms.insert(std::make_pair<std::string,std::vector<int64_t>>("esb",{}));
    m = &ms["esb"];
    std::cout<<"start measuring"<<std::endl;
    for(int i=0; i< measures; i++){
        start = std::chrono::high_resolution_clock::now();
        ARROW_ASSIGN_OR_RAISE(call_res,arrow::compute::CallFunction("add",{table2->GetColumnByName("numbers"),table2->GetColumnByName("numbers")}));
        m->push_back(getTime());
        std::cout<<"built-in elemwise sum res: "<<call_res.chunked_array()->ToString()<<std::endl;
        std::cout<<"built-in elemwise sum exec time: "<<m->back()<<"ns"<<std::endl;
    }
    ms.insert(std::make_pair<std::string,std::vector<int64_t>>("esc",{}));
    m = &ms["esc"];
    for(int i=0; i< measures; i++){
        ARROW_ASSIGN_OR_RAISE(e,
            arrow::compute::GetFunctionExecutor("add_elemwise",{table2->GetColumnByName("numbers"),table2->GetColumnByName("numbers")},NULLPTR,funcReg.get()));
        start = std::chrono::high_resolution_clock::now();
        ARROW_ASSIGN_OR_RAISE(call_res,
            e->Execute({table2->GetColumnByName("numbers"),table2->GetColumnByName("numbers")}));
        m->push_back(getTime());
        std::cout<<"custom elemwise sum res: "<<call_res.chunked_array()->ToString()<<std::endl;
        std::cout<<"custom elemwise sum exec time: "<<m->back()<<"ns"<<std::endl;
    }
    // 10.:
    // one json file containing array of obj
    // naming convention: <func><type> 
    //  e.g. aso (aggregate summation optimized)
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