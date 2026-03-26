#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionSparkbar.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{

class AggregateFunctionCombinatorSparkbar final : public IAggregateFunctionCombinator
{
public:
    String getName() const override
    {
        return "Sparkbar";
    }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Incorrect number of arguments for aggregate function with {} suffix. "
                "At least one argument (bucket key) is required", getName());

        /// Remove the first argument (bucket key), pass the rest to nested function
        return DataTypes(arguments.begin() + 1, arguments.end());
    }

    Array transformParameters(const Array & params) const override
    {
        /// Sparkbar combinator uses 1 or 3 parameters: width, or width + min_x + max_x
        /// These are consumed by the combinator, not passed to nested function
        if (params.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function with {} suffix requires at least 1 parameter (width)", getName());

        if (params.size() != 1 && params.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function with {} suffix requires 1 or 3 parameters (width) or (width, min_x, max_x), got {}",
                getName(), params.size());

        /// Return empty array - all parameters are consumed by the combinator
        return Array();
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Incorrect number of arguments for aggregate function with {} suffix. "
                "At least one argument (bucket key) is required", getName());

        if (params.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function with {} suffix requires at least 1 parameter (width)", getName());

        if (params.size() != 1 && params.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function with {} suffix requires 1 or 3 parameters, got {}", getName(), params.size());

        UInt64 width = params[0].safeGet<UInt64>();
        bool is_specified_range = (params.size() == 3);

        WhichDataType which{arguments[0]};

        if (which.isNativeUInt() || which.isDate() || which.isDateTime())
        {
            UInt64 begin_x = 0;
            UInt64 end_x = std::numeric_limits<UInt64>::max();

            if (is_specified_range)
            {
                begin_x = params[1].safeGet<UInt64>();
                end_x = params[2].safeGet<UInt64>();
            }

            return std::make_shared<AggregateFunctionSparkbar<UInt64>>(
                nested_function,
                static_cast<size_t>(width),
                is_specified_range,
                begin_x,
                end_x,
                arguments,
                params);
        }

        if (which.isNativeInt())
        {
            Int64 begin_x = std::numeric_limits<Int64>::min();
            Int64 end_x = std::numeric_limits<Int64>::max();

            if (is_specified_range)
            {
                if (!params[1].tryGet<Int64>(begin_x))
                    begin_x = static_cast<Int64>(params[1].safeGet<UInt64>());
                if (!params[2].tryGet<Int64>(end_x))
                    end_x = static_cast<Int64>(params[2].safeGet<UInt64>());
            }

            return std::make_shared<AggregateFunctionSparkbar<Int64>>(
                nested_function,
                static_cast<size_t>(width),
                is_specified_range,
                begin_x,
                end_x,
                arguments,
                params);
        }

        if (which.isDate32())
        {
            Int32 begin_x = std::numeric_limits<Int32>::min();
            Int32 end_x = std::numeric_limits<Int32>::max();

            if (is_specified_range)
            {
                Int64 tmp;
                if (params[1].tryGet<Int64>(tmp))
                    begin_x = static_cast<Int32>(tmp);
                else
                    begin_x = static_cast<Int32>(params[1].safeGet<UInt64>());
                if (params[2].tryGet<Int64>(tmp))
                    end_x = static_cast<Int32>(tmp);
                else
                    end_x = static_cast<Int32>(params[2].safeGet<UInt64>());
            }

            return std::make_shared<AggregateFunctionSparkbar<Int32>>(
                nested_function,
                static_cast<size_t>(width),
                is_specified_range,
                begin_x,
                end_x,
                arguments,
                params);
        }

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of the first argument for aggregate function with {} suffix. "
            "The type should be native integer, Date, Date32 or DateTime",
            arguments[0]->getName(), getName());
    }
};

}

void registerAggregateFunctionCombinatorSparkbar(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorSparkbar>());
}

}
