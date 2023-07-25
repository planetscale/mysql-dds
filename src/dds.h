#ifndef MYSQL_DDS_DDS_H
#define MYSQL_DDS_DDS_H

#include <optional>
#include <unordered_map>

struct Metadata {
    unsigned char version = 0;
    float sum = 0.0;
    unsigned long long count = 0;
    float gamma = 0.0;

    static std::optional<Metadata> Deserialize(const char *in, size_t length);

    bool Valid() const;

    bool Mergeable(const Metadata &other) const;

    double Mean() const;

    unsigned long long Count() const;

    double Sum() const;

};

struct Bucket {
    unsigned short key;
    unsigned long long count;

    bool operator<(const Bucket &other) const {
        return key < other.key;
    }

    bool operator==(const Bucket &other) const {
        return key == other.key && count == other.count;
    }
};

struct Decoder {
    const char *data;
    const char *end;
    unsigned long long prev_key = 0;

    Decoder(const char *data, size_t len);

    bool Empty() const;

    std::optional<uint64_t> ReadVarint(int max_len);

    std::optional<uint16_t> ReadVarint16();

    std::optional<uint64_t> ReadVarint64();

    std::optional<uint8_t> ReadFixedInt8();

    std::optional<float> ReadFloat();

    std::optional<Metadata> ReadMetadata();

    std::optional<Bucket> ReadBucket();

    std::optional<const char *> Advance(size_t len);

    size_t BytesLeft() const;
};

/*
 * Immutable Sketch. Buckets are stored as a vector which is quick to construct
 * when deserializing.
 *
 * The buckets field must be provided in order of Bucket#key.
 */
struct Sketch {
    const Metadata metadata;
    const std::vector<Bucket> buckets;

    static std::vector<uint8_t> EncodeVarint(uint64_t val);

    static std::optional<Sketch> Deserialize(const char *in, size_t length);

    double Quantile(double q) const;

    std::string Inspect() const;

    std::string Serialize() const;

    std::string JSON();
};

/*
 * Mutable container that can have multiple sketches Merged in. Stores bucket
 * as an unordered map which is efficient when many sketches are merged in
 * because merging pre-existing buckets is just a map lookup and an integer
 * increment.
 *
 * Can be converted to a Sketch object via #ToSketch. This requires sorting the
 * map keys to construct an ordered Bucket vector.
 */
struct Accumulator {
    std::optional<Metadata> metadata;
    std::unordered_map<unsigned short, unsigned long long> buckets;

    bool Merge(const char *in, size_t length);

    Sketch ToSketch() const;

    void Clear();
};

#endif //MYSQL_DDS_DDS_H
