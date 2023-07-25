#include <algorithm>
#include <cmath>
#include <cstring>
#include <sstream>
#include <vector>

#include "mysql.h"
#include "dds.h"

std::optional<Metadata> Metadata::Deserialize(const char *in, size_t length) {
    return Decoder(in, length).ReadMetadata();
}

bool Metadata::Valid() const {
    if (gamma <= 1.0) {
        return false;
    }

    if (version != 1) {
        return false;
    }

    if (count == 0) {
        return false;
    }

    return true;
}

bool Metadata::Mergeable(const Metadata &other) const {
    return gamma == other.gamma && version == other.version;
}

double Metadata::Mean() const {
    return sum / count;
}

unsigned long long Metadata::Count() const {
    return count;
}

double Metadata::Sum() const {
    return sum;
}

Decoder::Decoder(const char *in, size_t length) {
    data = in;
    end = in + length;
}

bool Decoder::Empty() const {
    return data >= end;
}

std::optional<const char *> Decoder::Advance(size_t length) {
    if (end - data < (long) length) {
        return {};
    }
    data += length;
    return data - length;
}

std::optional<uint16_t> Decoder::ReadVarint16() {
    return ReadVarint(3);
}

std::optional<uint64_t> Decoder::ReadVarint64() {
    return ReadVarint(10);
}

std::optional<uint64_t> Decoder::ReadVarint(int max_length) {
    auto max = end - data > max_length ? data + max_length : end;
    int shift = 0;
    uint64_t ret = 0;

    while (data < max) {
        ret += ((uint64_t) *data & 0x7f) << shift;

        if ((*data & 0x80) == 0) {
            data++;
            return ret;
        }

        shift += 7;
        data++;
    }

    return {};
}

std::optional<uint8_t> Decoder::ReadFixedInt8() {
    auto ptr = Advance(1);
    if (!ptr) return {};

    return *(const uint8_t *) ptr.value();
}

std::optional<float> Decoder::ReadFloat() {
    auto ptr = Advance(4);
    if (!ptr) return {};

    auto val = *(const float *) ptr.value();

    if (std::isinf(val) || std::isnan(val)) {
        return {};
    }
    return val;
}

std::optional<Metadata> Decoder::ReadMetadata() {
    auto version = ReadFixedInt8();
    if (!version) return {};

    auto gamma = ReadFloat();
    if (!gamma) return {};

    auto sum = ReadFloat();
    if (!sum) return {};

    auto count = ReadVarint64();
    if (!count) return {};

    auto metadata = Metadata{
            .version = version.value(),
            .sum = sum.value(),
            .count = count.value(),
            .gamma = gamma.value(),
    };

    if (!metadata.Valid()) return {};

    return metadata;
}

std::optional<Bucket> Decoder::ReadBucket() {
    auto key = ReadVarint16();
    if (!key) return {};

    auto count = ReadVarint64();
    if (!count) return {};

    unsigned short cur_key = prev_key + key.value();
    prev_key = cur_key;

    return Bucket{.key = cur_key, .count = count.value()};
}

size_t Decoder::BytesLeft() const {
    return end - data;
}

std::vector<uint8_t> Sketch::EncodeVarint(uint64_t val) {
    std::vector<uint8_t> ret;

    while (val & ~0x7F) {
        ret.push_back((val & 0xFF) | 0x80);
        val = val >> 7;
    }

    ret.push_back(val);

    return ret;
}

std::optional<Sketch> Sketch::Deserialize(const char *in, size_t length) {
    Decoder decoder = {in, length};

    auto metadata = decoder.ReadMetadata();
    if (!metadata) return {};

    std::vector<Bucket> buckets;

    // Smallest bucket is 2 bytes, so this gives us an upper bound on the number of buckets
    // Doing this allows us to avoid reallocations which has a measurable performance impact
    buckets.reserve(decoder.BytesLeft() / 2);

    while (!decoder.Empty()) {
        auto bucket = decoder.ReadBucket();
        if (!bucket) return {};
        buckets.push_back(bucket.value());
    }

    if (buckets.empty()) return {};
    buckets.shrink_to_fit();

    return Sketch{
            .metadata = metadata.value(),
            .buckets = buckets,
    };
}

double Sketch::Quantile(double q) const {
    if (q < 0) {
        q = 0;
    }
    unsigned long long rank = llround(q * (double) metadata.count);

    unsigned long long cuml_count = 0;
    unsigned short bucket_key = buckets.empty() ? 0 : buckets.back().key;

    for (auto bucket: buckets) {
        cuml_count += bucket.count;

        if (cuml_count >= rank) {
            bucket_key = bucket.key;
            break;
        }
    }

    return (2 * pow(metadata.gamma, bucket_key)) / (metadata.gamma + 1);
}

std::string Sketch::Inspect() const {
    std::ostringstream out;

    out << "Sketch<version: " << (unsigned short) metadata.version << ", sum:" << metadata.sum << ", count:"
        << metadata.count << ", gamma:"
        << metadata.gamma << ", bucket_count: " << buckets.size();
    out << ", buckets:{";
    for (auto bucket: buckets) {
        out << bucket.key << ": " << bucket.count << ", ";
    }
    out << "}>";

    return out.str();
}

std::string Sketch::JSON() {
    std::ostringstream out;

    out << "{"
        << "\"version\":" << (unsigned short) metadata.version << ","
        << "\"sum\":" << metadata.sum << ","
        << "\"count\":" << metadata.count << ","
        << "\"gamma\":"<< metadata.gamma << ",";

    out << "\"buckets\":{";
    for (auto &bucket: buckets) {
        out << "\"" << bucket.key << "\":" << bucket.count;
        if (&bucket != &buckets.back()) {
            out << ",";
        }
    }
    out << "}";

    out << "}";

    return out.str();
}

std::string Sketch::Serialize() const {
    std::ostringstream out;

    out.write((const char *) &metadata.version, 1);
    out.write((const char *) &metadata.gamma, 4);
    out.write((const char *) &metadata.sum, 4);

    auto varint = EncodeVarint(metadata.count);
    out.write((const char *) varint.data(), (long) varint.size());

    unsigned short prev_key = 0;
    for (auto bucket: buckets) {
        varint = EncodeVarint(bucket.key - prev_key);
        out.write((const char *) varint.data(), (long) varint.size());
        prev_key = bucket.key;

        varint = EncodeVarint(bucket.count);
        out.write((const char *) varint.data(), (long) varint.size());
    }

    return out.str();
}

bool Accumulator::Merge(const char *in, size_t length) {
    Decoder decoder = {in, length};

    auto in_metadata = decoder.ReadMetadata();
    if (!in_metadata) return false;

    if (metadata) {
        if (!metadata.value().Mergeable(in_metadata.value())) {
            return false;
        }

        metadata.value().sum += in_metadata.value().sum;
        metadata.value().count += in_metadata.value().count;
    } else {
        metadata = in_metadata;
    }

    while (!decoder.Empty()) {
        auto bucket = decoder.ReadBucket();
        if (!bucket) return false;

        buckets[bucket.value().key] += bucket.value().count;
    }

    if (buckets.empty()) {
        return false;
    }

    return true;
}

Sketch Accumulator::ToSketch() const {
    std::vector<Bucket> ordered;
    ordered.reserve(buckets.size());
    for (auto &[key, val]: buckets) {
        ordered.push_back({.key = key, .count = val});
    }
    std::sort(ordered.begin(), ordered.end());

    return {
            .metadata = metadata.value(),
            .buckets = ordered,
    };
}

void Accumulator::Clear() {
    metadata.reset();
    buckets.clear();
}

extern "C" [[maybe_unused]] bool dds_inspect_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
    if (args->arg_count != 1 || args->arg_type[0] != STRING_RESULT) {
        strcpy(message, "Requires exactly one sketch argument");
        return true;
    }

    initid->maybe_null = true;
    initid->max_length = 65535;
    initid->ptr = static_cast<char *>(static_cast<void *>(new std::string()));

    return false;
}

extern "C" [[maybe_unused]] char *dds_inspect(UDF_INIT *initid, UDF_ARGS *args, char *, unsigned long *length,
                                              unsigned char *is_null, char *error) {
    if (!args->args[0]) {
        return nullptr;
    }

    auto sketch = Sketch::Deserialize(args->args[0], args->lengths[0]);

    if (!sketch) {
        *error = 1;
        return nullptr;
    }

    auto *out = static_cast<std::string *>(static_cast<void *>(initid->ptr));
    out->assign(sketch.value().Inspect());

    *length = out->length();
    *is_null = 0;

    return out->data();
}
extern "C" [[maybe_unused]] void dds_inspect_deinit(UDF_INIT *initid) {
    delete static_cast<std::string *>(static_cast<void *>(initid->ptr));
}

struct Sum_Data {
    Accumulator acc;
    std::string serialized;
    bool set = false;
};

extern "C" [[maybe_unused]] bool dds_sum_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
    if (args->arg_count != 1 || args->arg_type[0] != STRING_RESULT) {
        strcpy(message, "Requires exactly one sketch argument");
        return true;
    }

    initid->maybe_null = true;
    initid->max_length = 65535;
    initid->ptr = static_cast<char *>(static_cast<void *>(new Sum_Data()));

    return false;
}

extern "C" [[maybe_unused]] void dds_sum_add(UDF_INIT *initid, UDF_ARGS *args, char *, char *error) {
    if (args->args[0] == nullptr) {
        return;
    }

    auto *data = static_cast<Sum_Data *>(static_cast<void *>(initid->ptr));

    auto success = data->acc.Merge(args->args[0], args->lengths[0]);
    if (!success) {
        *error = true;
        return;
    }

    data->set = true;
}

extern "C" [[maybe_unused]] void dds_sum_clear(UDF_INIT *initid, char *, char *) {
    auto *data = static_cast<Sum_Data *>(static_cast<void *>(initid->ptr));

    data->acc.Clear();
    data->set = false;
}

extern "C" [[maybe_unused]] char *
dds_sum(UDF_INIT *initid, UDF_ARGS *, char *result, unsigned long *length, char *is_null, char *) {
    auto *data = static_cast<Sum_Data *>(static_cast<void *>(initid->ptr));

    if (!data->set) {
        *is_null = true;
        return result;
    }

    data->serialized = data->acc.ToSketch().Serialize();

    *length = data->serialized.length();
    *is_null = 0;

    return data->serialized.data();
}

extern "C" [[maybe_unused]] void dds_sum_deinit(UDF_INIT *initid) {
    delete static_cast<Sum_Data *>(static_cast<void *>(initid->ptr));
}

extern "C" [[maybe_unused]] bool dds_quantile_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
    if (args->arg_count != 2) {
        strcpy(message, "Requires exactly two arguments");
        return true;
    }
    if (args->arg_type[0] != REAL_RESULT && args->arg_type[0] != INT_RESULT && args->arg_type[0] != DECIMAL_RESULT) {
        strcpy(message, "First argument must be a numeric quantile");
        return true;
    }
    if (args->arg_type[1] != STRING_RESULT) {
        strcpy(message, "Second argument must be a sketch");
        return true;
    }

    // Tell mysql to cast the quantile to a double
    args->arg_type[0] = REAL_RESULT;

    initid->maybe_null = true;
    return false;
}

extern "C" [[maybe_unused]] double dds_quantile(UDF_INIT *, UDF_ARGS *args, unsigned char *is_null, unsigned char *) {
    if (args->args[0] == nullptr || args->args[1] == nullptr) {
        *is_null = true;
        return 0.0;
    }

    auto sketch = Sketch::Deserialize(args->args[1], args->lengths[1]);
    if (!sketch) {
        *is_null = true;
        return 0.0;
    }

    double q = *((double *) args->args[0]);

    return sketch.value().Quantile(q);
}

extern "C" [[maybe_unused]] bool dds_merge_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
    if (args->arg_count != 2 || args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT) {
        strcpy(message, "Requires exactly two sketch arguments");
        return true;
    }

    initid->max_length = 65535;
    initid->ptr = static_cast<char *>(static_cast<void *>(new std::string()));

    return false;
}

extern "C" [[maybe_unused]] char *
dds_merge(UDF_INIT *initid, UDF_ARGS *args, char *, unsigned long *length, unsigned char *is_null, char *error) {
    if (!args->args[0] && !args->args[1]) {
        *is_null = true;
        return nullptr;
    }

    if (!args->args[0]) {
        *length = args->lengths[1];
        return args->args[1];
    }

    if (!args->args[1]) {
        *length = args->lengths[0];
        return args->args[0];
    }

    Accumulator acc;
    if (!acc.Merge(args->args[0], args->lengths[0])) {
        *error = true;
        return nullptr;
    }

    if (!acc.Merge(args->args[1], args->lengths[1])) {
        *error = true;
        return nullptr;
    }

    auto *out = static_cast<std::string *>(static_cast<void *>(initid->ptr));
    out->assign(acc.ToSketch().Serialize());
    *length = out->length();
    *is_null = 0;

    return out->data();
}

extern "C" [[maybe_unused]] void dds_merge_deinit(UDF_INIT *initid) {
    delete static_cast<std::string *>(static_cast<void *>(initid->ptr));
}

extern "C" [[maybe_unused]] bool dds_mean_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
    if (args->arg_count != 1 || args->arg_type[0] != STRING_RESULT) {
        strcpy(message, "Requires exactly one sketch argument");
        return true;
    }

    initid->maybe_null = true;

    return false;
}

extern "C" [[maybe_unused]] double dds_mean(UDF_INIT *, UDF_ARGS *args, unsigned char *is_null, unsigned char *) {
    if (args->args[0] == nullptr) {
        *is_null = true;
        return 0.0;
    }

    auto metadata = Metadata::Deserialize(args->args[0], args->lengths[0]);
    if (!metadata) {
        *is_null = true;
        return 0.0;
    }

    return metadata.value().Mean();
}

extern "C" [[maybe_unused]] bool dds_json_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
    if (args->arg_count != 1 || args->arg_type[0] != STRING_RESULT) {
        strcpy(message, "Requires exactly one sketch argument");
        return true;
    }

    initid->max_length = 65535;
    initid->ptr = static_cast<char *>(static_cast<void *>(new std::string()));

    return false;
}

extern "C" [[maybe_unused]] char *dds_json(UDF_INIT *initid, UDF_ARGS *args, char *, unsigned long *length,
                                              unsigned char *is_null, char *error) {
    if (!args->args[0]) {
        return nullptr;
    }

    auto sketch = Sketch::Deserialize(args->args[0], args->lengths[0]);

    if (!sketch) {
        *error = 1;
        return nullptr;
    }

    auto *out = static_cast<std::string *>(static_cast<void *>(initid->ptr));
    out->assign(sketch.value().JSON());

    *length = out->length();
    *is_null = 0;

    return out->data();
}

extern "C" [[maybe_unused]] void dds_json_deinit(UDF_INIT *initid) {
    delete static_cast<std::string *>(static_cast<void *>(initid->ptr));
}

extern "C" [[maybe_unused]] bool dds_invalid_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
    initid->maybe_null = true;

    if (args->arg_count != 1 || args->arg_type[0] != STRING_RESULT) {
        strcpy(message, "Requires exactly one sketch argument");
        return true;
    }

    return false;
}

extern "C" [[maybe_unused]] long long dds_invalid(UDF_INIT *, UDF_ARGS *args, unsigned char *is_null, unsigned char *) {
    if (!args->args[0]) {
        *is_null = 1;
        return 0;
    }

    *is_null = 0;
    auto sketch = Sketch::Deserialize(args->args[0], args->lengths[0]);

    return sketch.has_value() ? 0 : 1;
}

extern "C" [[maybe_unused]] bool dds_count_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
    if (args->arg_count != 1 || args->arg_type[0] != STRING_RESULT) {
        strcpy(message, "Requires exactly one sketch argument");
        return true;
    }

    initid->maybe_null = true;

    return false;
}

extern "C" [[maybe_unused]] long long dds_count(UDF_INIT *, UDF_ARGS *args, unsigned char *is_null, unsigned char *) {
    if (args->args[0] == nullptr) {
        *is_null = true;
        return 0;
    }

    auto metadata = Metadata::Deserialize(args->args[0], args->lengths[0]);
    if (!metadata) {
        *is_null = true;
        return 0;
    }

    return metadata.value().Count();
}

extern "C" [[maybe_unused]] bool dds_total_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
    if (args->arg_count != 1 || args->arg_type[0] != STRING_RESULT) {
        strcpy(message, "Requires exactly one sketch argument");
        return true;
    }

    initid->maybe_null = true;

    return false;
}

extern "C" [[maybe_unused]] double dds_total(UDF_INIT *, UDF_ARGS *args, unsigned char *is_null, unsigned char *) {
    if (args->args[0] == nullptr) {
        *is_null = true;
        return 0;
    }

    auto metadata = Metadata::Deserialize(args->args[0], args->lengths[0]);
    if (!metadata) {
        *is_null = true;
        return 0;
    }

    return metadata.value().Sum();
}
