#include <arrow/api.h>
#include <arrow/compute/api.h>

//if files will be gen put them in this relative dir 
#define BUILDDIR "build"

//macros for faster typing of std ptr stuff
#define U(t,n)\
    std::unique_ptr<t> n
#define S(t,n)\
    std::shared_ptr<t> n
#define M(t,n,a)\
    S(t,n) = std::make_shared<t>(t a);



//#pragma GCC push_options
//#pragma GCC optimize("O0")

// custom sum aggregate kernel 
class CustomSumKernelState: public arrow::compute::KernelState
{
public:
    // executing the kernel implementation, in this case its a summation
    // ctx not important, batch holds array datas or scalars over which the sum agg shall be exec.
    // lets assume arrays only
    arrow::Status consume(arrow::compute::KernelContext*, const arrow::compute::ExecSpan& execSpan)
    {

        //partially taken from internal impl
        const arrow::ArraySpan* arrData = &execSpan[0].array;
        // assumption: elem 0 refers to null-array where set bits indicate if the elem is null
        //TODO: why does this work if it has to work with arrow-arrays which contain null-values? 
        const int32_t* values = arrData->GetValues<int32_t>(1);
        #pragma GCC ivdep
        for(int64_t i = 0; i < arrData->length;i++)
        {
            sum+=values[i];
        }
        return arrow::Status::OK();
    }
    
    // merging this state with another state
    arrow::Status merge(arrow::compute::KernelContext*, arrow::compute::KernelState&& src)
    {   
        const CustomSumKernelState& other = static_cast<const CustomSumKernelState&>(src);
        this->sum += other.sum;
        return arrow::Status::OK();
    }

    // retrieving the execution results from the state
    arrow::Status finalize(arrow::compute::KernelContext*, arrow::Datum* datum)
    {
        datum->value = std::make_shared<arrow::Int64Scalar>(sum);
        return arrow::Status::OK();
    }

protected:
    long long sum = 0;
};
//#pragma GCC pop_options

// (simd) optimized custom sum aggregate kernel
#define REGSIZE 8
class OCustomSumKernelState: public CustomSumKernelState
{
public:
    // consume: simd optimized sum agg kernel execution
    arrow::Status consume(arrow::compute::KernelContext*, const arrow::compute::ExecSpan& execSpan)
    {
        const arrow::ArraySpan* arrData = &execSpan[0].array;
        const int32_t* values = arrData->GetValues<int32_t>(1);
        long long arr[REGSIZE] = {};
        if(arrData->length % REGSIZE != 0)
        {
            return arrow::Status::Invalid("invalid size");
        }

        for(int64_t i = 0; i < arrData->length;i+=REGSIZE)
        {
            #pragma unroll 
            for(int j=0;j<REGSIZE;j++){
                arr[j]+=values[i+j];
            }
        }
        for(int j=0;j<REGSIZE;j++){
            sum+=arr[j];
        }
        return arrow::Status::OK();
    }
};



//those functions do only exist to speed up typing
// init Kernel
// create kernel (impl)
template<typename KernelState>
arrow::Result<std::unique_ptr<arrow::compute::KernelState>> initK (arrow::compute::KernelContext*, const arrow::compute::KernelInitArgs&)
{
    return arrow::Result(std::unique_ptr<arrow::compute::KernelState>(new KernelState));
}
// consume: executing kernel
template<typename KernelState>
arrow::Status consumeK(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& execSpan)
{
    
    return static_cast<KernelState*>(ctx->state())->consume(ctx,execSpan);
}
// merge: combining two kernelstates
template<typename KernelState>
arrow::Status mergeK(arrow::compute::KernelContext* ctx, arrow::compute::KernelState&& src, arrow::compute::KernelState* dst)
{
    return static_cast<KernelState*>(dst)->merge(ctx,std::move(src));
}
// finalize: outputing results from kernel
template<typename KernelState>
arrow::Status finalizeK(arrow::compute::KernelContext* ctx, arrow::Datum* out)
{
    return static_cast<KernelState*>(ctx->state())->finalize(ctx,out);
}

// custom element wise add
// exec_span has input columns, res has the result column, all same size
arrow::Status add1(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& exec_span, arrow::compute::ExecResult* res)
{
    //get argument columns
    const int32_t* r1 = exec_span[0].array.GetValues<int32_t>(1);
    const int32_t* r2 = exec_span[1].array.GetValues<int32_t>(1);
    //iterate & write back sum of operands
    int32_t* out_data = res->array_span_mutable()->GetValues<int32_t>(1);
    for(int i = 0; i < exec_span.length; i++) //length refers to array arg lengths 
    {   
        *out_data++ = *r1++ + *r2++; //deref before inc
    }

    return arrow::Status::OK();
}