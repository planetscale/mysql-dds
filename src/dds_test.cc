#include <gtest/gtest.h>
#include <climits>
#include <cmath>
#include <array>
#include <unordered_map>
#include <map>
#include "dds.h"

TEST(Metadata, ValidChecksGamma) {
    Metadata metadata = {.version = 1, .count = 1};

    metadata.gamma = -1.0;
    EXPECT_FALSE(metadata.Valid());
    metadata.gamma = -0.1;
    EXPECT_FALSE(metadata.Valid());
    metadata.gamma = 0.0;
    EXPECT_FALSE(metadata.Valid());
    metadata.gamma = 1.0;
    EXPECT_FALSE(metadata.Valid());
    metadata.gamma = 1.01;
    EXPECT_TRUE(metadata.Valid());
    metadata.gamma = 2.0;
    EXPECT_TRUE(metadata.Valid());
}

TEST(Metadata, ValidChecksVersion) {
    Metadata metadata = {.count = 1, .gamma = 1.01};

    metadata.version = 0;
    EXPECT_FALSE(metadata.Valid());
    metadata.version = 1;
    EXPECT_TRUE(metadata.Valid());
    metadata.version = 2;
    EXPECT_FALSE(metadata.Valid());
}

TEST(Metadata, ValidChecksCount) {
    Metadata metadata = {.version = 1, .gamma = 1.01};

    metadata.count = 0;
    EXPECT_FALSE(metadata.Valid());
    metadata.count = 1;
    EXPECT_TRUE(metadata.Valid());
}


TEST(Metadata, Mergeable) {
    EXPECT_TRUE((Metadata{.version = 1, .gamma = 1.1}).Mergeable(Metadata{.version = 1, .gamma = 1.1}));
    EXPECT_FALSE((Metadata{.version = 1, .gamma = 1.1}).Mergeable(Metadata{.version = 1, .gamma = 1.2}));
    EXPECT_FALSE((Metadata{.version = 1, .gamma = 1.1}).Mergeable(Metadata{.version = 2, .gamma = 1.1}));
}

TEST(Metadata, Mean) {
    Metadata metadata = {
            .sum = 100,
            .count = 5,
    };

    EXPECT_FLOAT_EQ(metadata.Mean(), 20.0);
}

TEST(Bucket, LessThan) {
    EXPECT_TRUE(Bucket{.key = 1} < Bucket{.key = 2});
    EXPECT_FALSE(Bucket{.key = 2} < Bucket{.key = 2});
    EXPECT_FALSE(Bucket{.key = 3} < Bucket{.key = 2});
}

TEST(Decoder, Varint) {
    struct Decode_Varint_Exp {
        std::vector<unsigned char> bytes;
        std::optional<unsigned long long> want;
        int max_length = 10;
    };

    std::vector<Decode_Varint_Exp> expectations = {
            {.bytes = {}, .want = std::nullopt},
            {.bytes = {0x00}, .want =  0},
            {.bytes = {0x01}, .want =  1},
            {.bytes = {0b01111111}, .want =  127},
            {.bytes = {0b10000000}, .want =  std::nullopt}, // continuation bit set, but no bytes follow
            {.bytes = {0b10000000, 0x01}, .want =  128},
            {.bytes = {0xFF, 0b01111111}, .want =  16383},
            {.bytes = {0b10000000, 0b10000000, 0x01}, .want = 16384},
            {.bytes = {0b10000001, 0b10000000, 0x01}, .want = 16385},
            {.bytes = {0b11111110, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}, .want = ULLONG_MAX - 1},
            {.bytes = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}, .want = ULLONG_MAX},
            {.bytes = {0xFF, 0xFF, 0x01}, .want = 32767, .max_length = 3},
            {.bytes = {0xFF, 0xFF,
                       0x01}, .want = std::nullopt, .max_length = 2}, // valid 3 byte var-int, but max length is 2
    };
    for (auto expectation: expectations) {
        auto decoder = Decoder((char *) expectation.bytes.data(), expectation.bytes.size());
        auto result = decoder.ReadVarint(expectation.max_length);
        EXPECT_EQ(result, expectation.want);

        if (result) {
            EXPECT_EQ(decoder.data, (char *) expectation.bytes.data() + expectation.bytes.size());
        }
    }
}

TEST(Decoder, MultipleVarints) {
    unsigned char data[] = {0x00, 0xFF, 0x01, 0x01};
    auto dec = Decoder((char *) data, 4);

    EXPECT_EQ(dec.data, (char *) data);

    EXPECT_EQ(dec.ReadVarint(10), 0);
    EXPECT_EQ(dec.data, (char *) data + 1);

    EXPECT_EQ(dec.ReadVarint(10), 255);
    EXPECT_EQ(dec.data, (char *) data + 3);

    EXPECT_EQ(dec.ReadVarint(10), 1);
    EXPECT_EQ(dec.data, (char *) data + 4);

    EXPECT_EQ(dec.ReadVarint(10), std::nullopt);
}

TEST(Decoder, FixedInt) {
    unsigned char data[] = {0x00, 0x01, 0xFE, 0xFF};
    auto dec = Decoder((char *) data, 4);

    EXPECT_EQ(dec.data, (char *) data);

    EXPECT_EQ(dec.ReadFixedInt8(), 0);
    EXPECT_EQ(dec.data, (char *) data + 1);

    EXPECT_EQ(dec.ReadFixedInt8(), 1);
    EXPECT_EQ(dec.data, (char *) data + 2);

    EXPECT_EQ(dec.ReadFixedInt8(), 254);
    EXPECT_EQ(dec.data, (char *) data + 3);

    EXPECT_EQ(dec.ReadFixedInt8(), 255);
    EXPECT_EQ(dec.data, (char *) data + 4);

    EXPECT_EQ(dec.ReadFixedInt8(), std::nullopt);
}

std::vector<char> FloatBytes(float f) {
    char *p = (char *) &f;
    return {p[0], p[1], p[2], p[3]};
}

TEST(Decoder, Floats) {
    auto p_float_bytes = FloatBytes(1.020202);
    Decoder dec = Decoder(p_float_bytes.data(), sizeof(float));
    EXPECT_FLOAT_EQ(dec.ReadFloat().value(), 1.020202);

    auto nan_float_bytes = FloatBytes(std::numeric_limits<float>::quiet_NaN());
    dec = Decoder(nan_float_bytes.data(), sizeof(float));
    EXPECT_EQ(dec.ReadFloat(), std::nullopt);

    auto inf_float_bytes = FloatBytes(std::numeric_limits<float>::infinity());
    dec = Decoder(inf_float_bytes.data(), sizeof(float));
    EXPECT_EQ(dec.ReadFloat(), std::nullopt);

    auto neg_inf_float_bytes = FloatBytes(-std::numeric_limits<float>::infinity());
    dec = Decoder(neg_inf_float_bytes.data(), sizeof(float));
    EXPECT_EQ(dec.ReadFloat(), std::nullopt);
}

unsigned char serialized[] = {
        0x01, // version = 1, 8-bit unsigned int
        0xfb, 0x95, 0x82, 0x3f, // gamma = 1.020202, 32-bit float
        0xcd, 0xcc, 0x0c, 0x41, // sum = 8.8, 32-bit float
        0x04,   // count = 4, varint

        // bucket 0
        0x05, // key = 5, varint
        0x01, // val = 1, varint

        // bucket 1
        0x23, // delta = 35 (key = 40), varint
        0x02, // val = 2, varint

        // bucket 2
        0x14, // delta = 20 (key = 60), varint
        0x01 // val = 1, varint
};

TEST(Decoder, MetadataAndBuckets) {
    auto decoder = Decoder(reinterpret_cast<char *>(serialized), sizeof(serialized));

    auto metadata_result = decoder.ReadMetadata();
    EXPECT_TRUE(metadata_result.has_value());
    auto metadata = metadata_result.value();
    EXPECT_EQ(metadata.version, 1);
    EXPECT_FLOAT_EQ(metadata.sum, 8.8);
    EXPECT_EQ(metadata.count, 4);
    EXPECT_FLOAT_EQ(metadata.gamma, 1.020202);

    std::optional<Bucket> bucket_result;
    Bucket bucket;

    bucket_result = decoder.ReadBucket();
    EXPECT_TRUE(metadata_result.has_value());
    bucket = bucket_result.value();
    EXPECT_EQ(bucket, Bucket({.key = 5, .count = 1}));

    bucket_result = decoder.ReadBucket();
    EXPECT_TRUE(metadata_result.has_value());
    bucket = bucket_result.value();
    EXPECT_EQ(bucket, Bucket({.key = 40, .count = 2}));


    bucket_result = decoder.ReadBucket();
    EXPECT_TRUE(metadata_result.has_value());
    bucket = bucket_result.value();
    EXPECT_EQ(bucket, Bucket({.key = 60, .count = 1}));

    EXPECT_TRUE(decoder.Empty());
}

TEST(Sketch, SerializationRoundtrip) {
    Sketch original = {
            .metadata = {
                    .version = 1,
                    .sum = 10.0,
                    .count = 1,
                    .gamma = 1.1,
            },
            .buckets = std::vector<Bucket>({{.key = 1, .count = 10},
                                            {.key = 2, .count = 20}}),
    };
    auto original_bytes = original.Serialize();

    auto deserialized_result = Sketch::Deserialize(original_bytes.data(), original_bytes.length());
    EXPECT_TRUE(deserialized_result.has_value());
    auto deserialized = deserialized_result.value();

    EXPECT_EQ(original.metadata.version, deserialized.metadata.version);
    EXPECT_EQ(original.metadata.sum, deserialized.metadata.sum);
    EXPECT_EQ(original.metadata.count, deserialized.metadata.count);
    EXPECT_EQ(original.metadata.gamma, deserialized.metadata.gamma);
    EXPECT_EQ(original.buckets, deserialized.buckets);
}

TEST(Sketch, Quantile) {
    auto alpha = 0.01;
    auto gamma = float((1 + alpha) / (1 - alpha));
    auto samples = 100UL;

    // Calculate buckets in a map
    std::map<unsigned short, unsigned long long> map_buckets;
    for (int i = 1; i <= samples; ++i) {
        map_buckets[(unsigned short) ceil(log(i) / log(gamma))]++;
    }

    // Convert samples to an ordered vector of buckets
    std::vector<Bucket> vector_buckets;
    for (auto [key, count]: map_buckets) {
        vector_buckets.push_back({.key = key, .count = count});
    }

    Sketch sketch = {
            .metadata = {
                    .version = 1,
                    .sum = 0,
                    .count = samples,
                    .gamma = gamma,
            },
            .buckets = vector_buckets,
    };

    // Add 1/10,000th of a percent allowable error for float errors
    auto relative_error = alpha + 0.000001;

    for (int i = 1; i <= 100; ++i) {
        double estimate = sketch.Quantile(i / 100.0);
        EXPECT_LE(abs(estimate - i), (i * relative_error)) << "Relative error is too large at p" << i;
    }

    // Some literal tests for sanity
    EXPECT_LE(abs(sketch.Quantile(-1) - 1), (1 * relative_error)); // Quantiles < 0 are treated as 0
    EXPECT_LE(abs(sketch.Quantile(0) - 1), (1 * relative_error));
    EXPECT_LE(abs(sketch.Quantile(0.001) - 1), (1 * relative_error));
    EXPECT_LE(abs(sketch.Quantile(0.999) - 100), (100 * relative_error));
    EXPECT_LE(abs(sketch.Quantile(1) - 100), (100 * relative_error));
    EXPECT_LE(abs(sketch.Quantile(2) - 100), (100 * relative_error)); // Quantiles > 1 are treated as 1
}

TEST(Accumulator, Merge) {
    auto sketch_a_bytes = Sketch({
                                         .metadata = {
                                                 .version =  1,
                                                 .sum =  10.0,
                                                 .count = 6,
                                                 .gamma = 1.1,
                                         },
                                         .buckets = {
                                                 {.key = 1, .count = 1},
                                                 {.key = 2, .count = 2},
                                                 {.key = 3, .count = 3},
                                         },
                                 }).Serialize();

    auto sketch_b_bytes = Sketch({
                                         .metadata = {
                                                 .version =  1,
                                                 .sum =  20.0,
                                                 .count = 9,
                                                 .gamma = 1.1,
                                         },
                                         .buckets = {
                                                 {.key = 2, .count = 2},
                                                 {.key = 3, .count = 3},
                                                 {.key = 4, .count = 4},
                                         },
                                 }).Serialize();


    Accumulator acc;
    Metadata metadata;
    std::unordered_map<unsigned short, unsigned long long> expected_buckets;

    EXPECT_TRUE(acc.Merge(sketch_a_bytes.data(), sketch_a_bytes.length()));

    EXPECT_TRUE(acc.metadata.has_value());
    metadata = acc.metadata.value();
    EXPECT_FLOAT_EQ(metadata.version, 1);
    EXPECT_FLOAT_EQ(metadata.sum, 10.0);
    EXPECT_FLOAT_EQ(metadata.count, 6);
    EXPECT_FLOAT_EQ(metadata.gamma, 1.1);
    expected_buckets = {{1, 1},
                        {2, 2},
                        {3, 3}};
    EXPECT_EQ(acc.buckets, expected_buckets);

    EXPECT_TRUE(acc.Merge(sketch_b_bytes.data(), sketch_b_bytes.length()));

    EXPECT_TRUE(acc.metadata.has_value());
    metadata = acc.metadata.value();
    EXPECT_FLOAT_EQ(metadata.version, 1);
    EXPECT_FLOAT_EQ(metadata.sum, 30.0);
    EXPECT_FLOAT_EQ(metadata.count, 15);
    EXPECT_FLOAT_EQ(metadata.gamma, 1.1);
    expected_buckets = {{1, 1},
                        {2, 4},
                        {3, 6},
                        {4, 4}};
    EXPECT_EQ(acc.buckets, expected_buckets);
}

TEST(Accumulator, MergeInvalid) {
    Accumulator acc;
    EXPECT_FALSE(acc.Merge({}, 0));

    // meaningless values.  another 0 on the end is a valid 0->0 bucket.
    // 11 bytes is too few, so it fails as expected.
    unsigned char too_short[11] = {0, 0, 0, 0, 96, 181, 192, 74, 253, 127, 0};
    EXPECT_FALSE(acc.Merge(reinterpret_cast<char *>(too_short), sizeof(too_short)));
}

TEST(Accumulator, Clear) {
    auto acc = Accumulator{
            .metadata = Metadata{},
            .buckets = std::unordered_map<unsigned short, unsigned long long>{{1, 1}},
    };

    EXPECT_TRUE(acc.metadata.has_value());
    EXPECT_FALSE(acc.buckets.empty());
    acc.Clear();
    EXPECT_FALSE(acc.metadata.has_value());
    EXPECT_TRUE(acc.buckets.empty());
}