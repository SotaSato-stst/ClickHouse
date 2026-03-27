#pragma once

#include <array>
#include <string_view>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnString.h>
#include <Common/PODArray.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/arithmeticOverflow.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int BAD_ARGUMENTS;
}

/** Aggregate function combinator -Sparkbar applies another aggregate function to values
  * falling into buckets by the first argument, and then renders the results as a sparkbar string.
  *
  * Example: sumSparkbar(10)(date, amount) - sums amounts for each bucket and renders as sparkbar
  *          countSparkbar(10)(date) - counts values in each bucket and renders as sparkbar
  */
template <typename Key>
class AggregateFunctionSparkbar final : public IAggregateFunctionHelper<AggregateFunctionSparkbar<Key>>
{
private:
    static constexpr size_t BAR_LEVELS = 8;
    static constexpr size_t MAX_WIDTH = 1024;

    AggregateFunctionPtr nested_function;

    size_t width;
    bool is_specified_range;
    Key begin_x;
    Key end_x;

    size_t align_of_data;
    size_t size_of_data;

    /// Index of the bucket key column (first argument to the combinator)
    size_t key_col;

    size_t updateFrame(ColumnString::Chars & frame, Float64 value) const
    {
        static constexpr std::array<std::string_view, BAR_LEVELS + 1> bars{" ", "▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"};
        const auto & bar = (std::isnan(value) || value < 1 || value > BAR_LEVELS) ? bars[0] : bars[static_cast<UInt8>(value)];
        frame.insert(bar.begin(), bar.end());
        return bar.size();
    }

    void render(ColumnString & to_column, const PaddedPODArray<Float64> & values) const
    {
        auto & chars = to_column.getChars();
        auto & offsets = to_column.getOffsets();

        Float64 max_value = 0;
        for (const auto & v : values)
        {
            if (!std::isnan(v) && v > 0)
                max_value = std::max(max_value, v);
        }

        if (max_value == 0)
        {
            offsets.push_back(chars.size());
            return;
        }

        size_t sz = 0;
        for (auto v : values)
        {
            Float64 scaled = 0;
            if (!std::isnan(v) && v > 0)
                scaled = v / max_value * (BAR_LEVELS - 1) + 1;
            sz += updateFrame(chars, scaled);
        }

        offsets.push_back(chars.size());
    }

public:
    AggregateFunctionSparkbar(
        AggregateFunctionPtr nested_function_,
        size_t width_,
        bool is_specified_range_,
        Key begin_x_,
        Key end_x_,
        const DataTypes & arguments,
        const Array & params)
        : IAggregateFunctionHelper<AggregateFunctionSparkbar<Key>>{arguments, params, std::make_shared<DataTypeString>()}
        , nested_function{nested_function_}
        , width{width_}
        , is_specified_range{is_specified_range_}
        , begin_x{begin_x_}
        , end_x{end_x_}
        , align_of_data{nested_function->alignOfData()}
        , size_of_data{(nested_function->sizeOfData() + align_of_data - 1) / align_of_data * align_of_data}
        , key_col{0}
    {
        if (width < 2 || width > MAX_WIDTH)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter width must be in range [2, {}]", MAX_WIDTH);

        if (is_specified_range && begin_x >= end_x)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter min_x must be less than max_x");
    }

    String getName() const override
    {
        return nested_function->getName() + "Sparkbar";
    }

    bool isState() const override
    {
        return nested_function->isState();
    }

    bool isVersioned() const override
    {
        return nested_function->isVersioned();
    }

    size_t getVersionFromRevision(size_t revision) const override
    {
        return nested_function->getVersionFromRevision(revision);
    }

    size_t getDefaultVersion() const override
    {
        return nested_function->getDefaultVersion();
    }

    bool allocatesMemoryInArena() const override
    {
        return nested_function->allocatesMemoryInArena();
    }

    bool hasTrivialDestructor() const override
    {
        return nested_function->hasTrivialDestructor();
    }

    size_t sizeOfData() const override
    {
        /// We store `width` instances of the nested function state plus min/max tracking for auto-range
        /// Layout: [min_x: Key] [max_x: Key] [has_data: UInt8] [padding] [nested_states: width * size_of_data]
        return sizeof(Key) * 2 + sizeof(UInt8) + (align_of_data - ((sizeof(Key) * 2 + sizeof(UInt8)) % align_of_data)) % align_of_data + width * size_of_data;
    }

    size_t alignOfData() const override
    {
        return std::max(align_of_data, alignof(Key));
    }

    AggregateDataPtr getNestedPlace(AggregateDataPtr __restrict place, size_t bucket) const
    {
        size_t header_size = sizeof(Key) * 2 + sizeof(UInt8);
        size_t padding = (align_of_data - (header_size % align_of_data)) % align_of_data;
        return place + header_size + padding + bucket * size_of_data;
    }

    Key & minKey(AggregateDataPtr __restrict place) const
    {
        return *reinterpret_cast<Key *>(place);
    }

    Key & maxKey(AggregateDataPtr __restrict place) const
    {
        return *reinterpret_cast<Key *>(place + sizeof(Key));
    }

    UInt8 & hasData(AggregateDataPtr __restrict place) const
    {
        return *reinterpret_cast<UInt8 *>(place + sizeof(Key) * 2);
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        minKey(place) = std::numeric_limits<Key>::max();
        maxKey(place) = std::numeric_limits<Key>::lowest();
        hasData(place) = 0;

        for (size_t i = 0; i < width; ++i)
        {
            try
            {
                nested_function->create(getNestedPlace(place, i));
            }
            catch (...)
            {
                for (size_t j = 0; j < i; ++j)
                    nested_function->destroy(getNestedPlace(place, j));
                throw;
            }
        }
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        for (size_t i = 0; i < width; ++i)
            nested_function->destroy(getNestedPlace(place, i));
    }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override
    {
        for (size_t i = 0; i < width; ++i)
            nested_function->destroyUpToState(getNestedPlace(place, i));
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        Key key;

        if constexpr (static_cast<Key>(-1) < 0)
            key = static_cast<Key>(columns[key_col]->getInt(row_num));
        else
            key = static_cast<Key>(columns[key_col]->getUInt(row_num));

        Key from_x = is_specified_range ? begin_x : minKey(place);
        Key to_x = is_specified_range ? end_x : maxKey(place);

        /// Track min/max for auto-range
        if (!is_specified_range)
        {
            if (key < minKey(place))
                minKey(place) = key;
            if (key > maxKey(place))
                maxKey(place) = key;
            hasData(place) = 1;
        }

        /// For range-specified mode, skip out-of-range keys
        if (is_specified_range && (key < begin_x || key > end_x))
            return;

        /// Calculate bucket index
        /// We need to recalculate bucket positions during insertResultInto for auto-range mode
        /// For specified range, we can calculate now
        if (is_specified_range)
        {
            size_t bucket = 0;
            if (from_x < to_x)
            {
                Key delta = to_x - from_x;
                if (delta < std::numeric_limits<Key>::max())
                    delta = delta + 1;
                Float64 w = static_cast<Float64>(width);
                bucket = std::min<size_t>(
                    static_cast<size_t>(w / static_cast<Float64>(delta) * static_cast<Float64>(key - from_x)),
                    width - 1);
            }

            /// Create column array without the first argument (the key column)
            /// The nested function receives columns starting from index 1
            nested_function->add(getNestedPlace(place, bucket), columns + 1, row_num, arena);
        }
        else
        {
            /// For auto-range mode, we store all values in bucket 0 initially
            /// This is a simplification - ideally we'd re-bucket during merge
            /// For now, store in a single bucket and handle in insertResultInto
            hasData(place) = 1;
            nested_function->add(getNestedPlace(place, 0), columns + 1, row_num, arena);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        /// Merge min/max tracking
        const Key rhs_min = *reinterpret_cast<const Key *>(rhs);
        const Key rhs_max = *reinterpret_cast<const Key *>(rhs + sizeof(Key));
        const UInt8 rhs_has_data = *reinterpret_cast<const UInt8 *>(rhs + sizeof(Key) * 2);

        if (rhs_has_data)
        {
            if (rhs_min < minKey(place))
                minKey(place) = rhs_min;
            if (rhs_max > maxKey(place))
                maxKey(place) = rhs_max;
            hasData(place) = 1;
        }

        for (size_t i = 0; i < width; ++i)
            nested_function->merge(getNestedPlace(place, i), getNestedPlace(const_cast<AggregateDataPtr>(rhs), i), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        writeBinaryLittleEndian(minKey(const_cast<AggregateDataPtr>(place)), buf);
        writeBinaryLittleEndian(maxKey(const_cast<AggregateDataPtr>(place)), buf);
        writeBinaryLittleEndian(hasData(const_cast<AggregateDataPtr>(place)), buf);

        for (size_t i = 0; i < width; ++i)
            nested_function->serialize(getNestedPlace(const_cast<AggregateDataPtr>(place), i), buf, version);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        readBinaryLittleEndian(minKey(place), buf);
        readBinaryLittleEndian(maxKey(place), buf);
        readBinaryLittleEndian(hasData(place), buf);

        for (size_t i = 0; i < width; ++i)
            nested_function->deserialize(getNestedPlace(place, i), buf, version, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        auto & to_column = assert_cast<ColumnString &>(to);

        if (!hasData(place) && !is_specified_range)
        {
            /// No data - return empty string
            to_column.insertData("", 0);
            return;
        }

        /// Extract values from each bucket
        PaddedPODArray<Float64> bucket_values(width, 0);

        /// Create a temporary column to receive nested results
        auto result_type = nested_function->getResultType();
        auto temp_column = result_type->createColumn();

        for (size_t i = 0; i < width; ++i)
        {
            nested_function->insertResultInto(getNestedPlace(place, i), *temp_column, arena);
        }

        /// Convert results to Float64 for rendering
        for (size_t i = 0; i < width; ++i)
        {
            if (temp_column->isNullAt(i))
            {
                bucket_values[i] = std::nan("");
            }
            else
            {
                bucket_values[i] = temp_column->getFloat64(i);
            }
        }

        render(to_column, bucket_values);
    }

    AggregateFunctionPtr getNestedFunction() const override { return nested_function; }
};

}
