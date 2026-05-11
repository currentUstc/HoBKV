// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "bucket_table_builder.h"

#include <algorithm>
#include <cassert>
#include <cstring>

#include "TimberSaw/options.h"
#include "util/coding.h"

namespace TimberSaw {

namespace {
inline size_t AlignUp(size_t value, size_t align) {
	return (value + align - 1) / align * align;
}
}  // namespace

BucketTableBuilder::BucketTableBuilder(const std::shared_ptr<ValueZone>& value_zone)
	: value_zone_(value_zone) {}

BucketTableBuilder::BucketTableBuilder(const std::shared_ptr<ValueZone>& value_zone,
                                       const std::shared_ptr<RDMA_Manager>& rdma_mg,
                                       uint8_t target_node_id)
	: value_zone_(value_zone), rdma_mg_(rdma_mg), target_node_id_(target_node_id) {}

void BucketTableBuilder::Add(const Slice& key, const Slice& value) {
    // user key
	assert(!finished_);
	const uint64_t key_index = static_cast<uint64_t>(key.Index());
	const uint64_t local_offset_units = local_offset_bytes_ / kAlignBytes;

	const size_t value_padded = AlignUp(value.size(), kAlignBytes);
	const uint64_t total_size = static_cast<uint64_t>(
		kAlignBytes + kAlignBytes + kAlignBytes + value_padded + kAlignBytes);

	PendingEntry entry{
		key_index,
		local_offset_bytes_,
	};
	pending_entries_.push_back(entry);

	if (update_batches_.empty() ||
	    update_batches_.back().buffer.size() + kUpdatePairBytes > static_cast<size_t>(RDMA_WRITE_BLOCK)) {
		UpdateBatch batch;
		batch.buffer.reserve(static_cast<size_t>(RDMA_WRITE_BLOCK));
		update_batches_.push_back(std::move(batch));
	}

	// TODO: 这里的 local_offset 需要在之后换算为全局的 offset ，
	PutFixed64(&update_batches_.back().buffer, key_index);
	PutFixed64(&update_batches_.back().buffer, local_offset_units);
	update_batches_.back().kv_count += 1;

	PutFixed64(&value_zone_buffer_, total_size);
	PutFixed64(&value_zone_buffer_, key_index);
	PutFixed64(&value_zone_buffer_, 0);  // sequence_number
	value_zone_buffer_.append(value.data(), value.size());
	if (value_padded > value.size()) {
		const size_t pad = value_padded - value.size();
		value_zone_buffer_.append(pad, '\0');
	}
	PutFixed32(&value_zone_buffer_, 0);  // next
	PutFixed32(&value_zone_buffer_, 0);  // prev

	local_offset_bytes_ += total_size;
}

Status BucketTableBuilder::Finish() {
	if (finished_) {
		return Status::OK();
	}
	finished_ = true;

	if (!value_zone_) {
		return Status::InvalidArgument("BucketTableBuilder: value zone is null");
	}

	if (!value_zone_->HasSpace(value_zone_buffer_.size())) {
		return Status::IOError("BucketTableBuilder: not enough value zone space");
	}

	Status s = value_zone_->BatchedPut(value_zone_buffer_, &value_zone_base_offset_units_);
	if (!s.ok()) {
		return s;
	}

	bucket_updates_.clear();
	bucket_updates_.reserve(pending_entries_.size());
	for (const auto& entry : pending_entries_) {
		const uint64_t absolute_offset_units =
			value_zone_base_offset_units_ + (entry.local_offset_bytes / kAlignBytes);
		bucket_updates_.push_back({entry.key_index, absolute_offset_units});
	}

	if (rdma_mg_ && !bucket_updates_.empty()) {
		for (auto& batch : update_batches_) {
			if (batch.kv_count == 0) {
				continue;
			}

			const size_t max_kv_count = batch.buffer.size() / kUpdatePairBytes;
			const size_t kv_count = std::min(batch.kv_count, max_kv_count);
			const size_t buffer_size = kv_count * kUpdatePairBytes;
			char* ptr = batch.buffer.data();
			for (size_t i = 0; i < kv_count; ++i) {
				const uint64_t local_units = DecodeFixed64(ptr + sizeof(uint64_t));
				EncodeFixed64(ptr + sizeof(uint64_t), value_zone_base_offset_units_ + local_units);
				ptr += kUpdatePairBytes;
			}

			ibv_mr send_mr = {};
			ibv_mr receive_mr = {};
			ibv_mr updates_mr = {};
			rdma_mg_->Allocate_Local_RDMA_Slot(send_mr, Message);
			rdma_mg_->Allocate_Local_RDMA_Slot(receive_mr, Message);
			rdma_mg_->Allocate_Local_RDMA_Slot(updates_mr, FlushBuffer);
			if (buffer_size > updates_mr.length) {
				rdma_mg_->Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
				rdma_mg_->Deallocate_Local_RDMA_Slot(receive_mr.addr, Message);
				rdma_mg_->Deallocate_Local_RDMA_Slot(updates_mr.addr, FlushBuffer);
				return Status::IOError("BucketTableBuilder: updates buffer exceeds RDMA buffer");
			}

			// TODO: 一定需要这里的 memcpy 吗？
			std::memcpy(updates_mr.addr, batch.buffer.data(), buffer_size);

			RDMA_Request* send_pointer = (RDMA_Request*)send_mr.addr;
			send_pointer->command = flush_helper;
			send_pointer->content.flush_helper.buffer_size = buffer_size;
			send_pointer->content.flush_helper.kv_count = kv_count;
			send_pointer->buffer = receive_mr.addr;
			send_pointer->rkey = receive_mr.rkey;
			send_pointer->buffer_large = updates_mr.addr;
			send_pointer->rkey_large = updates_mr.rkey;
			send_pointer->imm_num = 0;

			RDMA_Reply* receive_pointer = (RDMA_Reply*)receive_mr.addr;
			*receive_pointer = {};
			asm volatile("sfence\n" : :);
			asm volatile("lfence\n" : :);
			asm volatile("mfence\n" : :);

			rdma_mg_->post_send<RDMA_Request>(&send_mr, target_node_id_, std::string("main"));
			ibv_wc wc[2] = {};
			if (rdma_mg_->poll_completion(wc, 1, std::string("main"), true, target_node_id_)) {
				rdma_mg_->Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
				rdma_mg_->Deallocate_Local_RDMA_Slot(receive_mr.addr, Message);
				rdma_mg_->Deallocate_Local_RDMA_Slot(updates_mr.addr, FlushBuffer);
				return Status::IOError("BucketTableBuilder: flush helper send failed");
			}
			if (!rdma_mg_->poll_reply_buffer(receive_pointer)) {
				rdma_mg_->Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
				rdma_mg_->Deallocate_Local_RDMA_Slot(receive_mr.addr, Message);
				rdma_mg_->Deallocate_Local_RDMA_Slot(updates_mr.addr, FlushBuffer);
				return Status::IOError("BucketTableBuilder: flush helper reply timeout");
			}

			rdma_mg_->Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
			rdma_mg_->Deallocate_Local_RDMA_Slot(receive_mr.addr, Message);
			rdma_mg_->Deallocate_Local_RDMA_Slot(updates_mr.addr, FlushBuffer);
		}
	}

	return Status::OK();
}

}  // namespace TimberSaw
