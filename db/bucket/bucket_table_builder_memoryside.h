// Memory-side bucket table updater. The bucket memory must be pre-allocated.

#ifndef BUCKET_TABLE_BUILDER_MEMORYSIDE_H
#define BUCKET_TABLE_BUILDER_MEMORYSIDE_H

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>

#include "TimberSaw/options.h"
#include "TimberSaw/status.h"

namespace TimberSaw {

class BucketTableBuilderMemorySide {
public:
	BucketTableBuilderMemorySide(char* bucket_data, size_t bucket_bytes)
		: bucket_data_(bucket_data), bucket_bytes_(bucket_bytes) {}

	Status UpdateSlot(uint64_t slot_index, uint64_t offset_units) {
		if (bucket_data_ == nullptr || bucket_bytes_ == 0) {
			return Status::InvalidArgument("BucketTableBuilderMemorySide: bucket data is null");
		}
		const uint64_t max_slots = bucket_bytes_ / SLOT_SIZE;
		if (slot_index >= max_slots) {
			return Status::InvalidArgument("BucketTableBuilderMemorySide: slot index out of range");
		}
		if (offset_units > std::numeric_limits<uint32_t>::max()) {
			return Status::InvalidArgument("BucketTableBuilderMemorySide: offset exceeds 32-bit slot");
		}

		const uint32_t offset = static_cast<uint32_t>(offset_units);
		std::memcpy(bucket_data_ + slot_index * SLOT_SIZE, &offset, SLOT_SIZE);
		return Status::OK();
	}

	Status ReadSlot(uint64_t slot_index, uint64_t* offset_units) const {
		if (offset_units == nullptr) {
			return Status::InvalidArgument("BucketTableBuilderMemorySide: offset_units is null");
		}
		if (bucket_data_ == nullptr || bucket_bytes_ == 0) {
			return Status::InvalidArgument("BucketTableBuilderMemorySide: bucket data is null");
		}
		const uint64_t max_slots = bucket_bytes_ / SLOT_SIZE;
		if (slot_index >= max_slots) {
			return Status::InvalidArgument("BucketTableBuilderMemorySide: slot index out of range");
		}

		uint32_t offset = 0;
		std::memcpy(&offset, bucket_data_ + slot_index * SLOT_SIZE, SLOT_SIZE);
		*offset_units = offset;
		return Status::OK();
	}

	uint64_t MaxSlots() const {
		if (bucket_bytes_ == 0) {
			return 0;
		}
		return bucket_bytes_ / SLOT_SIZE;
	}

private:
	char* bucket_data_ = nullptr;
	size_t bucket_bytes_ = 0;
};

}  // namespace TimberSaw

#endif  // BUCKET_TABLE_BUILDER_MEMORYSIDE_H
