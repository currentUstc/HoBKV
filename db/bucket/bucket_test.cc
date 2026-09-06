#include "TimberSaw/slice.h"

#include <cstdint>
#include <limits>
#include <string>

#include "db/bucket/value_zone.h"
#include "db/bucket/bucket_table.h"
#include "db/bucket/bucket_table_handler.h"
#include "gtest/gtest.h"
#include "util/coding.h"

namespace TimberSaw {

TEST(SliceTest, PreservesArbitraryBytesAndBytewiseOrdering) {
    const char binary[] = {'a', '\0', 'b'};
    Slice bytes(binary, sizeof(binary));
    uint64_t index = 123;
    EXPECT_FALSE(bytes.TryGetIndex(&index));
    EXPECT_EQ(123U, index);

    EXPECT_LT(Slice("10").compare(Slice("2")), 0);
    EXPECT_NE(Slice("01"), Slice("1"));
}

TEST(SliceTest, ParsesDecimalIndexesSafely) {
    uint64_t index = 0;
    EXPECT_TRUE(Slice("00042").TryGetIndex(&index));
    EXPECT_EQ(42U, index);

    const std::string max_value = std::to_string(std::numeric_limits<uint64_t>::max());
    EXPECT_TRUE(Slice(max_value).TryGetIndex(&index));
    EXPECT_EQ(std::numeric_limits<uint64_t>::max(), index);
    EXPECT_FALSE(Slice("18446744073709551616").TryGetIndex(&index));
    EXPECT_FALSE(Slice("").TryGetIndex(&index));
}

TEST(ValueZoneEntryTest, PreservesKeyValueSequenceAndDeletionType) {
    const std::string key = "00000000000000000042";
    const std::string value("a\0b", 3);
    std::string encoded;
    uint64_t key_index = 0;
    ASSERT_TRUE(AppendValueZoneEntry(Slice(key), Slice(value), 77, true,
                                     &encoded, &key_index).ok());
    EXPECT_EQ(42U, key_index);
    ASSERT_GE(encoded.size(), 56U);
    EXPECT_EQ(encoded.size(), DecodeFixed64(encoded.data()));
    EXPECT_EQ(42U, DecodeFixed64(encoded.data() + 8));
    EXPECT_EQ(77U, DecodeFixed64(encoded.data() + 16));
    EXPECT_EQ(1U, DecodeFixed64(encoded.data() + 24));
    EXPECT_EQ(key.size(), DecodeFixed64(encoded.data() + 32));
    EXPECT_EQ(value.size(), DecodeFixed64(encoded.data() + 40));
    EXPECT_EQ(key, encoded.substr(48, key.size()));
    const size_t padded_key_size =
        (key.size() + VALUE_ZONE_OFFSET_UNIT - 1) / VALUE_ZONE_OFFSET_UNIT * VALUE_ZONE_OFFSET_UNIT;
    EXPECT_EQ(value, encoded.substr(48 + padded_key_size, value.size()));
}

TEST(BucketTableHandlerTest, RetainsSharedBucketWithoutCopyingOwnership) {
    ibv_mr mr = {};
    mr.addr = reinterpret_cast<void*>(0x1000);
    mr.length = BUCKET_SIZE;
    std::weak_ptr<BucketTable> weak_bucket;
    {
        BucketTableHandler handler(BUCKET_NUMBER, BUCKET_NUMBER * SLOT_NUM_PER_BUCKET);
        auto bucket = std::make_shared<BucketTable>(Slice("0"), Slice("134217728"), mr, nullptr);
        weak_bucket = bucket;
        ASSERT_TRUE(handler.AssignRegionToNewBucket(0, bucket).ok());
        bucket.reset();
        EXPECT_FALSE(weak_bucket.expired());

        ibv_mr retained_mr = {};
        ASSERT_TRUE(handler.GetBucketMRForRegion(0, &retained_mr).ok());
        EXPECT_EQ(mr.addr, retained_mr.addr);
    }
    EXPECT_TRUE(weak_bucket.expired());
}

}  // namespace TimberSaw

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
