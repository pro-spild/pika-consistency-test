//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <iostream>
#include <algorithm>
#include <climits>
#include <limits>
#include <memory>

#include <fmt/core.h>
#include <glog/logging.h>

#include "pstd/include/pika_codis_slot.h"
#include "src/base_key_format.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"
#include "src/strings_filter.h"
#include "src/redis.h"
#include "storage/util.h"

namespace storage {
Status Redis::ScanStringsKeyNum(KeyInfo* key_info) {
  uint64_t keys = 0;
  uint64_t expires = 0;
  uint64_t ttl_sum = 0;
  uint64_t invaild_keys = 0;

  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  pstd::TimeType curtime = pstd::NowMillis();

  // Note: This is a string type and does not need to pass the column family as
  // a parameter, use the default column family
  rocksdb::Iterator* iter = db_->NewIterator(iterator_options);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (!ExpectedMetaValue(DataType::kStrings, iter->value().ToString())) {
      continue;
    }
    ParsedStringsValue parsed_strings_value(iter->value());
    if (parsed_strings_value.IsStale()) {
      invaild_keys++;
    } else {
      keys++;
      if (!parsed_strings_value.IsPermanentSurvival()) {
        expires++;
        ttl_sum += parsed_strings_value.Etime() - curtime;
      }
    }
  }
  delete iter;

  key_info->keys = keys;
  key_info->expires = expires;
  key_info->avg_ttl = (expires != 0) ? ttl_sum / expires : 0;
  key_info->invaild_keys = invaild_keys;
  return Status::OK();
}

Status Redis::Append(const Slice& key, const Slice& value, int32_t* ret, int64_t* expired_timestamp_millsec, std::string& out_new_value) {
  std::string old_value;
  *ret = 0;
  *expired_timestamp_millsec = 0;
  ScopeRecordLock l(lock_mgr_, key);

  BaseKey base_key(key);
  Status s = db_->Get(default_read_options_, base_key.Encode(), &old_value);
  if (s.ok() && !ExpectedMetaValue(DataType::kStrings, old_value)) {
    if (ExpectedStale(old_value)) {
      s = Status::NotFound();
    } else {
     return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kStrings)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(old_value))]);
    }
  }
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&old_value);
    if (parsed_strings_value.IsStale()) {
      *ret = static_cast<int32_t>(value.size());
      StringsValue strings_value(value);
      return db_->Put(default_write_options_, base_key.Encode(), strings_value.Encode());
    } else {
      uint64_t timestamp = parsed_strings_value.Etime();
      std::string old_user_value = parsed_strings_value.UserValue().ToString();
      std::string new_value = old_user_value + value.ToString();
      out_new_value = new_value;
      StringsValue strings_value(new_value);
      strings_value.SetEtime(timestamp);
      *ret = static_cast<int32_t>(new_value.size());
      *expired_timestamp_millsec = timestamp;
      return db_->Put(default_write_options_, base_key.Encode(), strings_value.Encode());
    }
  } else if (s.IsNotFound()) {
    *ret = static_cast<int32_t>(value.size());
    StringsValue strings_value(value);
    *expired_timestamp_millsec = 0;
    return db_->Put(default_write_options_, base_key.Encode(), strings_value.Encode());
  }
  return s;
}

int GetBitCount(const unsigned char* value, int64_t bytes) {
  int bit_num = 0;
  static const unsigned char bitsinbyte[256] = {
      0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2,
      3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3,
      3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5,
      6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4,
      3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4,
      5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6,
      6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8};
  for (int i = 0; i < bytes; i++) {
    bit_num += bitsinbyte[static_cast<unsigned int>(value[i])];
  }
  return bit_num;
}

Status Redis::BitCount(const Slice& key, int64_t start_offset, int64_t end_offset, int32_t* ret,
                          bool have_range) {
  *ret = 0;
  std::string value;

  BaseKey base_key(key);
  Status s = db_->Get(default_read_options_, base_key.Encode(), &value);
  if (s.ok() && !ExpectedMetaValue(DataType::kStrings, value)) {
    if (ExpectedStale(value)) {
      s = Status::NotFound();
    } else {
     return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kStrings)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(value))]);
    }
  }
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (parsed_strings_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      parsed_strings_value.StripSuffix();
      const auto bit_value = reinterpret_cast<const unsigned char*>(value.data());
      auto value_length = static_cast<int64_t>(value.length());
      if (have_range) {
        if (start_offset < 0) {
          start_offset = start_offset + value_length;
        }
        if (end_offset < 0) {
          end_offset = end_offset + value_length;
        }
        if (start_offset < 0) {
          start_offset = 0;
        }
        if (end_offset < 0) {
          end_offset = 0;
        }

        if (end_offset >= value_length) {
          end_offset = value_length - 1;
        }
        if (start_offset > end_offset) {
          return Status::OK();
        }
      } else {
        start_offset = 0;
        end_offset = std::max(value_length - 1, static_cast<int64_t>(0));
      }
      *ret = GetBitCount(bit_value + start_offset, end_offset - start_offset + 1);
    }
  } else {
    return s;
  }
  return Status::OK();
}

std::string BitOpOperate(BitOpType op, const std::vector<std::string>& src_values, int64_t max_len) {
  char byte;
  char output;
  auto dest_value = std::make_unique<char[]>(max_len);
  for (int64_t j = 0; j < max_len; j++) {
    if (j < static_cast<int64_t>(src_values[0].size())) {
      output = src_values[0][j];
    } else {
      output = 0;
    }
    if (op == kBitOpNot) {
      output = static_cast<char>(~output);
    }
    for (size_t i = 1; i < src_values.size(); i++) {
      if (static_cast<int64_t>(src_values[i].size()) - 1 >= j) {
        byte = src_values[i][j];
      } else {
        byte = 0;
      }
      switch (op) {
        case kBitOpNot:
          break;
        case kBitOpAnd:
          output = static_cast<char>(output & byte);
          break;
        case kBitOpOr:
          output = static_cast<char>(output | byte);
          break;
        case kBitOpXor:
          output = static_cast<char>(output ^ byte);
          break;
        case kBitOpDefault:
          break;
      }
    }
    dest_value[j] = output;
  }
  std::string dest_str(dest_value.get(), max_len);
  return dest_str;
}

Status Redis::BitOp(BitOpType op, const std::string& dest_key, const std::vector<std::string>& src_keys, std::string& value_to_dest, int64_t* ret) {
  Status s;
  if (op == kBitOpNot && src_keys.size() != 1) {
    return Status::InvalidArgument("the number of source keys is not right");
  } else if (src_keys.empty()) {
    return Status::InvalidArgument("the number of source keys is not right");
  }

  int64_t max_len = 0;
  int64_t value_len = 0;
  std::vector<std::string> src_values;
  for (const auto & src_key : src_keys) {
    std::string value;
    BaseKey base_key(src_key);
    s = db_->Get(default_read_options_, base_key.Encode(), &value);
    if (s.ok() && !ExpectedMetaValue(DataType::kStrings, value)) {
      if (ExpectedStale(value)) {
        s = Status::NotFound();
      } else {
        return Status::InvalidArgument(
          "WRONGTYPE, key: " + dest_key + ", expect type: " +
          DataTypeStrings[static_cast<int>(DataType::kStrings)] + ", get type: " +
          DataTypeStrings[static_cast<int>(GetMetaValueType(value))]);
      }
    }
    if (s.ok()) {
      ParsedStringsValue parsed_strings_value(&value);
      if (parsed_strings_value.IsStale()) {
        src_values.emplace_back("");
        value_len = 0;
      } else {
        parsed_strings_value.StripSuffix();
        src_values.push_back(value);
        value_len = static_cast<int64_t>(value.size());
      }
    } else if (s.IsNotFound()) {
      src_values.emplace_back("");
      value_len = 0;
    } else {
      return s;
    }
    max_len = std::max(max_len, value_len);
  }

  std::string dest_value = BitOpOperate(op, src_values, max_len);
  value_to_dest = dest_value;
  *ret = static_cast<int64_t>(dest_value.size());

  StringsValue strings_value(Slice(dest_value.c_str(), max_len));
  ScopeRecordLock l(lock_mgr_, dest_key);
  BaseKey base_dest_key(dest_key);
  return db_->Put(default_write_options_, base_dest_key.Encode(), strings_value.Encode());
}

Status Redis::Decrby(const Slice& key, int64_t value, int64_t* ret) {
  std::string old_value;
  std::string new_value;
  ScopeRecordLock l(lock_mgr_, key);

  BaseKey base_key(key);
  Status s = db_->Get(default_read_options_, base_key.Encode(), &old_value);
  if (s.ok() && !ExpectedMetaValue(DataType::kStrings, old_value)) {
    if (ExpectedStale(old_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kStrings)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(old_value))]);
    }
  }
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&old_value);
    if (parsed_strings_value.IsStale()) {
      *ret = -value;
      new_value = std::to_string(*ret);
      StringsValue strings_value(new_value);
      return db_->Put(default_write_options_, base_key.Encode(), strings_value.Encode());
    } else {
      uint64_t timestamp = parsed_strings_value.Etime();
      std::string old_user_value = parsed_strings_value.UserValue().ToString();
      char* end = nullptr;
      errno = 0;
      int64_t ival = strtoll(old_user_value.c_str(), &end, 10);
      if (errno == ERANGE || *end != 0) {
        return Status::Corruption("Value is not a integer");
      }
      if ((value >= 0 && LLONG_MIN + value > ival) || (value < 0 && LLONG_MAX + value < ival)) {
        return Status::InvalidArgument("Overflow");
      }
      *ret = ival - value;
      new_value = std::to_string(*ret);
      StringsValue strings_value(new_value);
      strings_value.SetEtime(timestamp);
      return db_->Put(default_write_options_, base_key.Encode(), strings_value.Encode());
    }
  } else if (s.IsNotFound()) {
    *ret = -value;
    new_value = std::to_string(*ret);
    StringsValue strings_value(new_value);
    return db_->Put(default_write_options_, base_key.Encode(), strings_value.Encode());
  } else {
    return s;
  }
}

Status Redis::Get(const Slice& key, std::string* value) {
  value->clear();

  BaseKey base_key(key);
  Status s = db_->Get(default_read_options_, base_key.Encode(), value);
  std::string meta_value = *value;
  if (s.ok() && !ExpectedMetaValue(DataType::kStrings, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
       return Status::InvalidArgument(
          "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
          DataTypeStrings[static_cast<int>(DataType::kStrings)] + ", get type: " +
          DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(value);
    if (parsed_strings_value.IsStale()) {
      value->clear();
      return Status::NotFound("Stale");
    } else {
      parsed_strings_value.StripSuffix();
    }
  }
  return s;
}

Status Redis::MGet(const Slice& key, std::string* value) {
  value->clear();

  BaseKey base_key(key);
  Status s = db_->Get(default_read_options_, base_key.Encode(), value);
  std::string meta_value = *value;
  if (s.ok() && !ExpectedMetaValue(DataType::kStrings, meta_value)) {
    return Status::NotFound();
  }
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(value);
    if (parsed_strings_value.IsStale()) {
      value->clear();
      return Status::NotFound("Stale");
    } else {
      parsed_strings_value.StripSuffix();
    }
  }
  return s;
}

void ClearValueAndSetTTL(std::string* value, int64_t* ttl, int64_t ttl_value) {
  value->clear();
  *ttl = ttl_value;
}

int64_t CalculateTTL(int64_t expiry_time) {
  pstd::TimeType current_time = pstd::NowMillis();
  return expiry_time - current_time >= 0 ? expiry_time - current_time : -2;
}

Status HandleParsedStringsValue(ParsedStringsValue& parsed_strings_value, std::string* value, int64_t* ttl_millsec) {
  if (parsed_strings_value.IsStale()) {
    ClearValueAndSetTTL(value, ttl_millsec, -2);
    return Status::NotFound("Stale");
  } else {
    parsed_strings_value.StripSuffix();
    int64_t expiry_time = parsed_strings_value.Etime();
    *ttl_millsec = (expiry_time == 0) ? -1 : CalculateTTL(expiry_time);
  }
  return Status::OK();
}

Status Redis::GetWithTTL(const Slice& key, std::string* value, int64_t* ttl_millsec) {
  value->clear();
  BaseKey base_key(key);
  Status s = db_->Get(default_read_options_, base_key.Encode(), value);
  std::string meta_value = *value;

  if (s.ok() && !ExpectedMetaValue(DataType::kStrings, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kStrings)] + " get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }

  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(value);
    return HandleParsedStringsValue(parsed_strings_value, value, ttl_millsec);
  } else if (s.IsNotFound()) {
    ClearValueAndSetTTL(value, ttl_millsec, -2);
  }

  return s;
}

Status Redis::MGetWithTTL(const Slice& key, std::string* value, int64_t* ttl_millsec) {
  value->clear();
  BaseKey base_key(key);
  Status s = db_->Get(default_read_options_, base_key.Encode(), value);
  std::string meta_value = *value;

  if (s.ok() && !ExpectedMetaValue(DataType::kStrings, meta_value)) {
    s = Status::NotFound();
  }

  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(value);
    return HandleParsedStringsValue(parsed_strings_value, value, ttl_millsec);
  } else if (s.IsNotFound()) {
    ClearValueAndSetTTL(value, ttl_millsec, -2);
  }

  return s;
}

Status Redis::GetBit(const Slice& key, int64_t offset, int32_t* ret) {
  std::string meta_value;

  BaseKey base_key(key);
  Status s = db_->Get(default_read_options_, base_key.Encode(), &meta_value);
  if (s.ok() || s.IsNotFound()) {
    std::string data_value;
    if (s.ok() && !ExpectedMetaValue(DataType::kStrings, meta_value)) {
      if (ExpectedStale(meta_value)) {
        s = Status::NotFound();
      } else {
        return Status::InvalidArgument(
          "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
          DataTypeStrings[static_cast<int>(DataType::kStrings)] + ", get type: " +
          DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
      }
    }
    if (s.ok()) {
      ParsedStringsValue parsed_strings_value(&meta_value);
      if (parsed_strings_value.IsStale()) {
        *ret = 0;
        return Status::OK();
      } else {
        data_value = parsed_strings_value.UserValue().ToString();
      }
    }
    size_t byte = offset >> 3;
    size_t bit = 7 - (offset & 0x7);
    if (byte + 1 > data_value.length()) {
      *ret = 0;
    } else {
      *ret = ((data_value[byte] & (1 << bit)) >> bit);
    }
  } else {
    return s;
  }
  return Status::OK();
}

Status Redis::Getrange(const Slice& key, int64_t start_offset, int64_t end_offset, std::string* ret) {
  *ret = "";
  std::string value;

  BaseKey base_key(key);
  Status s = db_->Get(default_read_options_, base_key.Encode(), &value);
  if (s.ok() && !ExpectedMetaValue(DataType::kStrings, value)) {
    if (ExpectedStale(value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kStrings)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(value))]);
    }
  }
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (parsed_strings_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      parsed_strings_value.StripSuffix();
      auto size = static_cast<int64_t>(value.size());
      int64_t start_t = start_offset >= 0 ? start_offset : size + start_offset;
      int64_t end_t = end_offset >= 0 ? end_offset : size + end_offset;
      if (start_t > size - 1 || (start_t != 0 && start_t > end_t) || (start_t != 0 && end_t < 0)) {
        return Status::OK();
      }
      if (start_t < 0) {
        start_t = 0;
      }
      if (end_t >= size) {
        end_t = size - 1;
      }
      if (start_t == 0 && end_t < 0) {
        end_t = 0;
      }
      *ret = value.substr(start_t, end_t - start_t + 1);
      return Status::OK();
    }
  } else {
    return s;
  }
}

Status Redis::GetrangeWithValue(const Slice& key, int64_t start_offset, int64_t end_offset,
                                std::string* ret, std::string* value, int64_t* ttl_millsec) {
  *ret = "";
  BaseKey base_key(key);
  Status s = db_->Get(default_read_options_, base_key.Encode(), value);
  std::string meta_value = *value;
  if (s.ok() && !ExpectedMetaValue(DataType::kStrings, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
       return Status::InvalidArgument(
          "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
          DataTypeStrings[static_cast<int>(DataType::kStrings)] + ", get type: " +
          DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(value);
    if (parsed_strings_value.IsStale()) {
      value->clear();
      *ttl_millsec = -2;
      return Status::NotFound("Stale");
    } else {
      parsed_strings_value.StripSuffix();
      // get ttl
      *ttl_millsec = parsed_strings_value.Etime();
      if (*ttl_millsec == 0) {
        *ttl_millsec = -1;
      } else {
        pstd::TimeType curtime = pstd::NowMillis();
        *ttl_millsec = *ttl_millsec - curtime >= 0 ? *ttl_millsec - curtime : -2;
      }

      int64_t size = value->size();
      int64_t start_t = start_offset >= 0 ? start_offset : size + start_offset;
      int64_t end_t = end_offset >= 0 ? end_offset : size + end_offset;
      if (start_t > size - 1 ||
          (start_t != 0 && start_t > end_t) ||
          (start_t != 0 && end_t < 0)
      ) {
        return Status::OK();
      }
      if (start_t < 0) {
        start_t  = 0;
      }
      if (end_t >= size) {
        end_t = size - 1;
      }
      if (start_t == 0 && end_t < 0) {
        end_t = 0;
      }
      *ret = value->substr(start_t, end_t-start_t+1);
      return Status::OK();
    }
  } else if (s.IsNotFound()) {
    value->clear();
    *ttl_millsec = -2;
  }
  return s;
}

Status Redis::GetSet(const Slice& key, const Slice& value, std::string* old_value) {
  ScopeRecordLock l(lock_mgr_, key);

  BaseKey base_key(key);
  Status s = db_->Get(default_read_options_, base_key.Encode(), old_value);
  std::string meta_value = *old_value;
  if (s.ok() && !ExpectedMetaValue(DataType::kStrings, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
       return Status::InvalidArgument(
          "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
          DataTypeStrings[static_cast<int>(DataType::kStrings)] + ", get type: " +
          DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(old_value);
    if (parsed_strings_value.IsStale()) {
      *old_value = "";
    } else {
      parsed_strings_value.StripSuffix();
    }
  } else if (!s.IsNotFound()) {
    return s;
  }
  StringsValue strings_value(value);
  return db_->Put(default_write_options_, base_key.Encode(), strings_value.Encode());
}

Status Redis::Incrby(const Slice& key, int64_t value, int64_t* ret, int64_t* expired_timestamp_millsec) {
  std::string old_value;
  std::string new_value;
  ScopeRecordLock l(lock_mgr_, key);
  BaseKey base_key(key);
  Status s = db_->Get(default_read_options_, base_key.Encode(), &old_value);
  char buf[32] = {0};
  if (s.ok() && !ExpectedMetaValue(DataType::kStrings, old_value)) {
    if (ExpectedStale(old_value)) {
      s = Status::NotFound();
    } else {
      return Status::InvalidArgument("WRONGTYPE, key: " + key.ToString() +
                                     ", expect type: " + DataTypeStrings[static_cast<int>(DataType::kStrings)] +
                                     ", get type: " + DataTypeStrings[static_cast<int>(GetMetaValueType(old_value))]);
    }
  }
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&old_value);
    if (parsed_strings_value.IsStale()) {
      *ret = value;
      Int64ToStr(buf, 32, value);
      StringsValue strings_value(buf);
      return db_->Put(default_write_options_, base_key.Encode(), strings_value.Encode());
    } else {
      uint64_t timestamp = parsed_strings_value.Etime();
      std::string old_user_value = parsed_strings_value.UserValue().ToString();
      char* end = nullptr;
      int64_t ival = strtoll(old_user_value.c_str(), &end, 10);
      if (*end != 0) {
        return Status::Corruption("Value is not a integer");
      }
      if ((value >= 0 && LLONG_MAX - value < ival) || (value < 0 && LLONG_MIN - value > ival)) {
        return Status::InvalidArgument("Overflow");
      }
      *ret = ival + value;
      new_value = std::to_string(*ret);
      StringsValue strings_value(new_value);
      strings_value.SetEtime(timestamp);
      *expired_timestamp_millsec = timestamp;
      return db_->Put(default_write_options_, base_key.Encode(), strings_value.Encode());
    }
  } else if (s.IsNotFound()) {
    *ret = value;
    Int64ToStr(buf, 32, value);
    StringsValue strings_value(buf);
    return db_->Put(default_write_options_, base_key.Encode(), strings_value.Encode());
  } else {
    return s;
  }
}

Status Redis::Incrbyfloat(const Slice& key, const Slice& value, std::string* ret, int64_t* expired_timestamp_sec) {
  std::string old_value;
  std::string new_value;
  *expired_timestamp_sec = 0;
  long double long_double_by;
  if (StrToLongDouble(value.data(), value.size(), &long_double_by) == -1) {
    return Status::Corruption("Value is not a vaild float");
  }

  BaseKey base_key(key);
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, base_key.Encode(), &old_value);
  if (s.ok() && !ExpectedMetaValue(DataType::kStrings, old_value)) {
    if (ExpectedStale(old_value)) {
      s = Status::NotFound();
    } else {
     return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kStrings)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(old_value))]);
    }
  }
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&old_value);
    if (parsed_strings_value.IsStale()) {
      LongDoubleToStr(long_double_by, &new_value);
      *ret = new_value;
      StringsValue strings_value(new_value);
      return db_->Put(default_write_options_, base_key.Encode(), strings_value.Encode());
    } else {
      uint64_t timestamp = parsed_strings_value.Etime();
      std::string old_user_value = parsed_strings_value.UserValue().ToString();
      long double total;
      long double old_number;
      if (StrToLongDouble(old_user_value.data(), old_user_value.size(), &old_number) == -1) {
        return Status::Corruption("Value is not a vaild float");
      }
      total = old_number + long_double_by;
      if (LongDoubleToStr(total, &new_value) == -1) {
        return Status::InvalidArgument("Overflow");
      }
      *ret = new_value;
      StringsValue strings_value(new_value);
      strings_value.SetEtime(timestamp);
      *expired_timestamp_sec = timestamp;
      return db_->Put(default_write_options_, base_key.Encode(), strings_value.Encode());
    }
  } else if (s.IsNotFound()) {
    LongDoubleToStr(long_double_by, &new_value);
    *ret = new_value;
    StringsValue strings_value(new_value);
    return db_->Put(default_write_options_, base_key.Encode(), strings_value.Encode());
  } else {
    return s;
  }
}

Status Redis::MSet(const std::vector<KeyValue>& kvs) {
  std::vector<std::string> keys;
  keys.reserve(kvs.size());
  for (const auto& kv : kvs) {
    keys.push_back(kv.key);
  }

  MultiScopeRecordLock ml(lock_mgr_, keys);
  rocksdb::WriteBatch batch;
  for (const auto& kv : kvs) {
    BaseKey base_key(kv.key);
    StringsValue strings_value(kv.value);
    batch.Put(base_key.Encode(), strings_value.Encode());
  }
  return db_->Write(default_write_options_, &batch);
}

Status Redis::MSetnx(const std::vector<KeyValue>& kvs, int32_t* ret) {
  Status s;
  bool exists = false;
  *ret = 0;
  std::string value;
  for (const auto & kv : kvs) {
    BaseKey base_key(kv.key);
    s = db_->Get(default_read_options_, base_key.Encode(), &value);
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
    if (s.ok() && !ExpectedStale(value)) {
      exists = true;
      break;
    }
    // when reaches here, either s is not found or s is ok but expired
  }
  if (!exists) {
    s = MSet(kvs);
    if (s.ok()) {
      *ret = 1;
    }
  }
  return s;
}

Status Redis::Set(const Slice& key, const Slice& value) {
  StringsValue strings_value(value);
  ScopeRecordLock l(lock_mgr_, key);

  BaseKey base_key(key);
  return db_->Put(default_write_options_, base_key.Encode(), strings_value.Encode());
}

Status Redis::Setxx(const Slice& key, const Slice& value, int32_t* ret, int64_t ttl_millsec) {
  bool not_found = true;
  std::string old_value;
  StringsValue strings_value(value);

  BaseKey base_key(key);
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, base_key.Encode(), &old_value);
  if (s.ok() && !ExpectedMetaValue(DataType::kStrings, old_value)) {
    if (ExpectedStale(old_value)) {
      s = Status::NotFound();
    } else {
     return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kStrings)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(old_value))]);
    }
  }
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(old_value);
    if (!parsed_strings_value.IsStale()) {
      not_found = false;
    }
  } else if (!s.IsNotFound()) {
    return s;
  }

  if (not_found) {
    *ret = 0;
    return s;
  } else {
    *ret = 1;
    if (ttl_millsec > 0) {
      strings_value.SetRelativeTimeInMillsec(ttl_millsec);
    }
    return db_->Put(default_write_options_, base_key.Encode(), strings_value.Encode());
  }
}

Status Redis::SetBit(const Slice& key, int64_t offset, int32_t on, int32_t* ret) {
  std::string meta_value;
  if (offset < 0) {
    return Status::InvalidArgument("offset < 0");
  }

  BaseKey base_key(key);
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, base_key.Encode(), &meta_value);
  if (s.ok() && !ExpectedMetaValue(DataType::kStrings, meta_value)) {
    if (ExpectedStale(meta_value)) {
      s = Status::NotFound();
    } else {
       return Status::InvalidArgument(
          "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
          DataTypeStrings[static_cast<int>(DataType::kStrings)] + ", get type: " +
          DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    }
  }
  if (s.ok() || s.IsNotFound()) {
    std::string data_value;
    uint64_t timestamp = 0;
    if (s.ok()) {
      ParsedStringsValue parsed_strings_value(&meta_value);
      if (!parsed_strings_value.IsStale()) {
        data_value = parsed_strings_value.UserValue().ToString();
        timestamp = parsed_strings_value.Etime();
      }
    }
    size_t byte = offset >> 3;
    size_t bit = 7 - (offset & 0x7);
    char byte_val;
    size_t value_lenth = data_value.length();
    if (byte + 1 > value_lenth) {
      *ret = 0;
      byte_val = 0;
    } else {
      *ret = ((data_value[byte] & (1 << bit)) >> bit);
      byte_val = data_value[byte];
    }
    if (*ret == on) {
      return Status::OK();
    }
    byte_val = static_cast<char>(byte_val & (~(1 << bit)));
    byte_val = static_cast<char>(byte_val | ((on & 0x1) << bit));
    if (byte + 1 <= value_lenth) {
      data_value.replace(byte, 1, &byte_val, 1);
    } else {
      data_value.append(byte + 1 - value_lenth - 1, 0);
      data_value.append(1, byte_val);
    }
    StringsValue strings_value(data_value);
    strings_value.SetEtime(timestamp);
    return db_->Put(rocksdb::WriteOptions(), base_key.Encode(), strings_value.Encode());
  } else {
    return s;
  }
}

Status Redis::Setex(const Slice& key, const Slice& value, int64_t ttl_millsec) {
  if (ttl_millsec <= 0) {
    return Status::InvalidArgument("invalid expire time");
  }
  StringsValue strings_value(value);
  auto s = strings_value.SetRelativeTimeInMillsec(ttl_millsec);
  if (s != Status::OK()) {
    return s;
  }

  BaseKey base_key(key);
  ScopeRecordLock l(lock_mgr_, key);
  return db_->Put(default_write_options_, base_key.Encode(), strings_value.Encode());
}

Status Redis::Setnx(const Slice& key, const Slice& value, int32_t* ret, int64_t ttl_millsec) {
  *ret = 0;
  std::string old_value;

  BaseKey base_key(key);
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, base_key.Encode(), &old_value);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  if (s.ok() && !ExpectedStale(old_value)) {
    return s;
  }
  // when reaches here, either s is not found or s is ok but expired
  s = Status::NotFound();

  StringsValue strings_value(value);
  if (ttl_millsec > 0) {
    strings_value.SetRelativeTimeInMillsec(ttl_millsec);
  }
  s = db_->Put(default_write_options_, base_key.Encode(), strings_value.Encode());
  if (s.ok()) {
    *ret = 1;
  }
  return s;
}

Status Redis::Setvx(const Slice& key, const Slice& value, const Slice& new_value, int32_t* ret,
                    int64_t ttl_millsec) {
  *ret = 0;
  std::string old_value;

  BaseKey base_key(key);
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, base_key.Encode(), &old_value);
  if (s.ok() && !ExpectedMetaValue(DataType::kStrings, old_value)) {
    if (ExpectedStale(old_value)) {
      s = Status::NotFound();
    } else {
     return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kStrings)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(old_value))]);
    }
  }
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&old_value);
    if (parsed_strings_value.IsStale()) {
      *ret = 0;
    } else {
      if (value.compare(parsed_strings_value.UserValue()) == 0) {
        StringsValue strings_value(new_value);
        if (ttl_millsec > 0) {
          strings_value.SetRelativeTimeInMillsec(ttl_millsec);
        }
        s = db_->Put(default_write_options_, base_key.Encode(), strings_value.Encode());
        if (!s.ok()) {
          return s;
        }
        *ret = 1;
      } else {
        *ret = -1;
      }
    }
  } else if (s.IsNotFound()) {
    *ret = 0;
  } else {
    return s;
  }
  return Status::OK();
}

Status Redis::Delvx(const Slice& key, const Slice& value, int32_t* ret) {
  *ret = 0;
  std::string old_value;

  BaseKey base_key(key);
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, base_key.Encode(), &old_value);
  if (s.ok() && !ExpectedMetaValue(DataType::kStrings, old_value)) {
    if (ExpectedStale(old_value)) {
      s = Status::NotFound();
    } else {
     return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kStrings)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(old_value))]);
    }
  }
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&old_value);
    if (parsed_strings_value.IsStale()) {
      *ret = 0;
      return Status::NotFound("Stale");
    } else {
      if (value.compare(parsed_strings_value.UserValue()) == 0) {
        *ret = 1;
        return db_->Delete(default_write_options_, base_key.Encode());
      } else {
        *ret = -1;
      }
    }
  } else if (s.IsNotFound()) {
    *ret = 0;
  }
  return s;
}

Status Redis::Setrange(const Slice& key, int64_t start_offset, const Slice& value, int32_t* ret) {
  std::string old_value;
  std::string new_value;
  if (start_offset < 0) {
    return Status::InvalidArgument("offset < 0");
  }
  ScopeRecordLock l(lock_mgr_, key);

  BaseKey base_key(key);
  Status s = db_->Get(default_read_options_, base_key.Encode(), &old_value);
  if (s.ok() && !ExpectedMetaValue(DataType::kStrings, old_value)) {
    if (ExpectedStale(old_value)) {
      s = Status::NotFound();
    } else {
     return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kStrings)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(old_value))]);
    }
  }
  if (s.ok()) {
    uint64_t timestamp = 0;
    ParsedStringsValue parsed_strings_value(&old_value);
    parsed_strings_value.StripSuffix();
    if (parsed_strings_value.IsStale()) {
      std::string tmp(start_offset, '\0');
      new_value = tmp.append(value.data());
      *ret = static_cast<int32_t>(new_value.length());
    } else {
      timestamp = parsed_strings_value.Etime();
      if (static_cast<size_t>(start_offset) > old_value.length()) {
        old_value.resize(start_offset);
        new_value = old_value.append(value.data());
      } else {
        std::string head = old_value.substr(0, start_offset);
        std::string tail;
        if ((start_offset + value.size()) < old_value.length()) {
          tail = old_value.substr(start_offset + value.size());
        }
        new_value = head + value.data() + tail;
      }
    }
    *ret = static_cast<int32_t>(new_value.length());
    StringsValue strings_value(new_value);
    strings_value.SetEtime(timestamp);
    return db_->Put(default_write_options_, base_key.Encode(), strings_value.Encode());
  } else if (s.IsNotFound()) {
    std::string tmp(start_offset, '\0');
    new_value = tmp.append(value.data());
    *ret = static_cast<int32_t>(new_value.length());
    StringsValue strings_value(new_value);
    return db_->Put(default_write_options_, base_key.Encode(), strings_value.Encode());
  }
  return s;
}

Status Redis::Strlen(const Slice& key, int32_t* len) {
  std::string value;
  Status s = Get(key, &value);
  if (s.ok()) {
    *len = static_cast<int32_t>(value.size());
  } else {
    *len = 0;
  }
  return s;
}

int32_t GetBitPos(const unsigned char* s, unsigned int bytes, int bit) {
  uint64_t word = 0;
  uint64_t skip_val = 0;
  auto value = const_cast<unsigned char*>(s);
  auto l = reinterpret_cast<uint64_t*>(value);
  int pos = 0;
  if (bit == 0) {
    skip_val = std::numeric_limits<uint64_t>::max();
  } else {
    skip_val = 0;
  }
  // skip 8 bytes at one time, find the first int64 that should not be skipped
  while (bytes >= sizeof(*l)) {
    if (*l != skip_val) {
      break;
    }
    l++;
    bytes = bytes - sizeof(*l);
    pos += static_cast<int32_t>(8 * sizeof(*l));
  }
  auto c = reinterpret_cast<unsigned char*>(l);
  for (size_t j = 0; j < sizeof(*l); j++) {
    word = word << 8;
    if (bytes != 0U) {
      word = word | *c;
      c++;
      bytes--;
    }
  }
  if (bit == 1 && word == 0) {
    return -1;
  }
  // set each bit of mask to 0 except msb
  uint64_t mask = std::numeric_limits<uint64_t>::max();
  mask = mask >> 1;
  mask = ~(mask);
  while (mask != 0U) {
    if (static_cast<int>((word & mask) != 0) == bit) {
      return pos;
    }
    pos++;
    mask = mask >> 1;
  }
  return pos;
}

Status Redis::BitPos(const Slice& key, int32_t bit, int64_t* ret) {
  Status s;
  std::string value;

  BaseKey base_key(key);
  s = db_->Get(default_read_options_, base_key.Encode(), &value);
  if (s.ok() && !ExpectedMetaValue(DataType::kStrings, value)) {
    if (ExpectedStale(value)) {
      s = Status::NotFound();
    } else {
     return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kStrings)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(value))]);
    }
  }
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (parsed_strings_value.IsStale()) {
      if (bit == 1) {
        *ret = -1;
      } else if (bit == 0) {
        *ret = 0;
      }
      return Status::NotFound("Stale");
    } else {
      parsed_strings_value.StripSuffix();
      const auto bit_value = reinterpret_cast<const unsigned char*>(value.data());
      auto value_length = static_cast<int64_t>(value.length());
      int64_t start_offset = 0;
      int64_t end_offset = std::max(value_length - 1, static_cast<int64_t>(0));
      int64_t bytes = end_offset - start_offset + 1;
      int64_t pos = GetBitPos(bit_value + start_offset, bytes, bit);
      if (pos == (8 * bytes) && bit == 0) {
        pos = -1;
      }
      if (pos != -1) {
        pos = pos + 8 * start_offset;
      }
      *ret = pos;
    }
  } else {
    return s;
  }
  return Status::OK();
}

Status Redis::BitPos(const Slice& key, int32_t bit, int64_t start_offset, int64_t* ret) {
  Status s;
  std::string value;

  BaseKey base_key(key);
  s = db_->Get(default_read_options_, base_key.Encode(), &value);
  if (s.ok() && !ExpectedMetaValue(DataType::kStrings, value)) {
    if (ExpectedStale(value)) {
      s = Status::NotFound();
    } else {
     return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kStrings)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(value))]);
    }
  }
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (parsed_strings_value.IsStale()) {
      if (bit == 1) {
        *ret = -1;
      } else if (bit == 0) {
        *ret = 0;
      }
      return Status::NotFound("Stale");
    } else {
      parsed_strings_value.StripSuffix();
      const auto bit_value = reinterpret_cast<const unsigned char*>(value.data());
      auto value_length = static_cast<int64_t>(value.length());
      int64_t end_offset = std::max(value_length - 1, static_cast<int64_t>(0));
      if (start_offset < 0) {
        start_offset = start_offset + value_length;
      }
      if (start_offset < 0) {
        start_offset = 0;
      }
      if (start_offset > end_offset) {
        *ret = -1;
        return Status::OK();
      }
      if (start_offset > value_length - 1) {
        *ret = -1;
        return Status::OK();
      }
      int64_t bytes = end_offset - start_offset + 1;
      int64_t pos = GetBitPos(bit_value + start_offset, bytes, bit);
      if (pos == (8 * bytes) && bit == 0) {
        pos = -1;
      }
      if (pos != -1) {
        pos = pos + 8 * start_offset;
      }
      *ret = pos;
    }
  } else {
    return s;
  }
  return Status::OK();
}

Status Redis::BitPos(const Slice& key, int32_t bit, int64_t start_offset, int64_t end_offset, int64_t* ret) {
  Status s;
  std::string value;

  BaseKey base_key(key);
  s = db_->Get(default_read_options_, base_key.Encode(), &value);
  if (s.ok() && !ExpectedMetaValue(DataType::kStrings, value)) {
    if (ExpectedStale(value)) {
      s = Status::NotFound();
    } else {
     return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kStrings)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(value))]);
    }
  }
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (parsed_strings_value.IsStale()) {
      if (bit == 1) {
        *ret = -1;
      } else if (bit == 0) {
        *ret = 0;
      }
      return Status::NotFound("Stale");
    } else {
      parsed_strings_value.StripSuffix();
      const auto bit_value = reinterpret_cast<const unsigned char*>(value.data());
      auto value_length = static_cast<int64_t>(value.length());
      if (start_offset < 0) {
        start_offset = start_offset + value_length;
      }
      if (start_offset < 0) {
        start_offset = 0;
      }
      if (end_offset < 0) {
        end_offset = end_offset + value_length;
      }
      // converting to int64_t just avoid warning
      if (end_offset > static_cast<int64_t>(value.length()) - 1) {
        end_offset = value_length - 1;
      }
      if (end_offset < 0) {
        end_offset = 0;
      }
      if (start_offset > end_offset) {
        *ret = -1;
        return Status::OK();
      }
      if (start_offset > value_length - 1) {
        *ret = -1;
        return Status::OK();
      }
      int64_t bytes = end_offset - start_offset + 1;
      int64_t pos = GetBitPos(bit_value + start_offset, bytes, bit);
      if (pos == (8 * bytes) && bit == 0) {
        pos = -1;
      }
      if (pos != -1) {
        pos = pos + 8 * start_offset;
      }
      *ret = pos;
    }
  } else {
    return s;
  }
  return Status::OK();
}

//TODO(wangshaoyi): timestamp uint64_t
Status Redis::PKSetexAt(const Slice& key, const Slice& value, int64_t time_stamp_millsec_) {
  StringsValue strings_value(value);
  if (time_stamp_millsec_ < 0) {
    time_stamp_millsec_ = pstd::NowMillis() - 1;
  }
  BaseKey base_key(key);
  ScopeRecordLock l(lock_mgr_, key);
  strings_value.SetEtime(uint64_t(time_stamp_millsec_));
  return db_->Put(default_write_options_, base_key.Encode(), strings_value.Encode());
}

Status Redis::StringsExpire(const Slice& key, int64_t ttl_millsec, std::string&& prefetch_meta) {
  std::string value(std::move(prefetch_meta));

  BaseKey base_key(key);
  ScopeRecordLock l(lock_mgr_, key);
  Status s;
  // value is empty means no meta value get before,
  // we should get meta first
  if (value.empty()) {
    Status s = db_->Get(default_read_options_, base_key.Encode(), &value);
    if (s.ok() && !ExpectedMetaValue(DataType::kStrings, value)) {
      if (ExpectedStale(value)) {
        s = Status::NotFound();
      } else {
       return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kStrings)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(value))]);
      }
    }
  }
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (parsed_strings_value.IsStale()) {
      return Status::NotFound("Stale");
    }
    if (ttl_millsec > 0) {
      parsed_strings_value.SetRelativeTimestamp(ttl_millsec);
      return db_->Put(default_write_options_, base_key.Encode(), value);
    } else {
      return db_->Delete(default_write_options_, base_key.Encode());
    }
  }
  return s;
}

Status Redis::StringsDel(const Slice& key, std::string&& prefetch_meta) {
  std::string value(std::move(prefetch_meta));
  ScopeRecordLock l(lock_mgr_, key);
  BaseKey base_key(key);
  Status s;

  // value is empty means no meta value get before,
  // we should get meta first
  if (value.empty()) {
    Status s = db_->Get(default_read_options_, base_key.Encode(), &value);
    if (s.ok() && !ExpectedMetaValue(DataType::kStrings, value)) {
      if (ExpectedStale(value)) {
        s = Status::NotFound();
      } else {
       return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kStrings)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(value))]);
      }
    }
  }
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (parsed_strings_value.IsStale()) {
      return Status::NotFound("Stale");
    }
    return db_->Delete(default_write_options_, base_key.Encode());
  }
  return s;
}

Status Redis::StringsExpireat(const Slice& key, int64_t timestamp_millsec, std::string&& prefetch_meta) {
  std::string value(std::move(prefetch_meta));
  ScopeRecordLock l(lock_mgr_, key);
  BaseKey base_key(key);
  Status s;

  // value is empty means no meta value get before,
  // we should get meta first
  if (value.empty()) {
    Status s = db_->Get(default_read_options_, base_key.Encode(), &value);
    if (s.ok() && !ExpectedMetaValue(DataType::kStrings, value)) {
      if (ExpectedStale(value)) {
        s = Status::NotFound();
      } else {
       return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kStrings)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(value))]);
      }
    }
  }
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (parsed_strings_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      if (timestamp_millsec > 0) {
        parsed_strings_value.SetEtime(static_cast<uint64_t>(timestamp_millsec));
        return db_->Put(default_write_options_, base_key.Encode(), value);
      } else {
        return db_->Delete(default_write_options_, base_key.Encode());
      }
    }
  }
  return s;
}

Status Redis::StringsPersist(const Slice& key, std::string&& prefetch_meta) {
  std::string value(std::move(prefetch_meta));
  ScopeRecordLock l(lock_mgr_, key);
  BaseKey base_key(key);
  Status s;

  // value is empty means no meta value get before,
  // we should get meta first
  if (value.empty()) {
    s = db_->Get(default_read_options_, base_key.Encode(), &value);
    if (s.ok() && !ExpectedMetaValue(DataType::kStrings, value)) {
      if (ExpectedStale(value)) {
        s = Status::NotFound();
      } else {
       return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kStrings)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(value))]);
      }
    }
  }
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (parsed_strings_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      uint64_t timestamp = parsed_strings_value.Etime();
      if (timestamp == 0) {
        return Status::NotFound("Not have an associated timeout");
      } else {
        parsed_strings_value.SetEtime(0);
        return db_->Put(default_write_options_, base_key.Encode(), value);
      }
    }
  }
  return s;
}

Status Redis::StringsTTL(const Slice& key, int64_t* ttl_millsec, std::string&& prefetch_meta) {
  std::string value(std::move(prefetch_meta));
  ScopeRecordLock l(lock_mgr_, key);
  BaseKey base_key(key);
  Status s;

  // value is empty means no meta value get before,
  // we should get meta first
  if (value.empty()) {
    s = db_->Get(default_read_options_, base_key.Encode(), &value);
    if (s.ok() && !ExpectedMetaValue(DataType::kStrings, value)) {
      if (ExpectedStale(value)) {
        s = Status::NotFound();
      } else {
       return Status::InvalidArgument(
        "WRONGTYPE, key: " + key.ToString() + ", expect type: " +
        DataTypeStrings[static_cast<int>(DataType::kStrings)] + ", get type: " +
        DataTypeStrings[static_cast<int>(GetMetaValueType(value))]);
      }
    }
  }
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (parsed_strings_value.IsStale()) {
      *ttl_millsec = -2;
      return Status::NotFound("Stale");
    } else {
      *ttl_millsec = parsed_strings_value.Etime();
      if (*ttl_millsec == 0) {
        *ttl_millsec = -1;
      } else {
        pstd::TimeType curtime = pstd::NowMillis();
        *ttl_millsec = *ttl_millsec - curtime >= 0 ? *ttl_millsec - curtime : -2;
      }
    }
  } else if (s.IsNotFound()) {
    *ttl_millsec = -2;
  }
  return s;
}

void Redis::ScanStrings() {
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;
  auto current_time = static_cast<int32_t>(time(nullptr));

  LOG(INFO) << "***************" << "rocksdb instance: " << index_ << " " << "String Data***************";
  auto iter = db_->NewIterator(iterator_options);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (!ExpectedMetaValue(DataType::kStrings, iter->value().ToString())) {
      continue;
    }
    ParsedBaseKey parsed_strings_key(iter->key());
    ParsedStringsValue parsed_strings_value(iter->value());
    int32_t survival_time = 0;
    if (parsed_strings_value.Etime() != 0) {
      survival_time =
          parsed_strings_value.Etime() - current_time > 0 ? parsed_strings_value.Etime() - current_time : -1;
    }
    LOG(INFO) << fmt::format("[key : {:<30}] [value : {:<30}] [timestamp : {:<10}] [version : {}] [survival_time : {}]", parsed_strings_key.Key().ToString(),
                             parsed_strings_value.UserValue().ToString(), parsed_strings_value.Etime(), parsed_strings_value.Version(),
                             survival_time);

  }
  delete iter;
}

rocksdb::Status Redis::Exists(const Slice& key) {
  std::string meta_value;
  uint64_t llen = 0;
  int32_t ret = 0;
  BaseMetaKey base_meta_key(key);
  std::vector<storage::IdMessage> id_messages;
  storage::StreamScanArgs arg;
  storage::StreamUtils::StreamParseIntervalId("-", arg.start_sid, &arg.start_ex, 0);
  storage::StreamUtils::StreamParseIntervalId("+", arg.end_sid, &arg.end_ex, UINT64_MAX);
  rocksdb::Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    auto type = static_cast<DataType>(static_cast<uint8_t>(meta_value[0]));
    switch (type) {
      case DataType::kSets:
        return SCard(key, &ret, std::move(meta_value));
      case DataType::kZSets:
        return ZCard(key, &ret, std::move(meta_value));
      case DataType::kHashes:
        return HLen(key, &ret, std::move(meta_value));
      case DataType::kLists:
        return LLen(key, &llen, std::move(meta_value));
      case DataType::kStreams:
        return XRange(key, arg, id_messages, std::move(meta_value));
      case DataType::kStrings:
        return ExpectedStale(meta_value) ? rocksdb::Status::NotFound() : rocksdb::Status::OK();
      default:
        return rocksdb::Status::NotFound();
    }
  }
  return rocksdb::Status::NotFound();
}

rocksdb::Status Redis::Del(const Slice& key) {
  std::string meta_value;
  BaseMetaKey base_meta_key(key);
  rocksdb::Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    auto type = static_cast<DataType>(static_cast<uint8_t>(meta_value[0]));
    switch (type) {
      case DataType::kSets:
        return SetsDel(key, std::move(meta_value));
      case DataType::kZSets:
        return ZsetsDel(key, std::move(meta_value));
      case DataType::kHashes:
        return HashesDel(key, std::move(meta_value));
      case DataType::kLists:
        return ListsDel(key, std::move(meta_value));
      case DataType::kStrings:
        return StringsDel(key, std::move(meta_value));
      case DataType::kStreams:
        return StreamsDel(key, std::move(meta_value));
      default:
        return rocksdb::Status::NotFound();
    }
  }
  return rocksdb::Status::NotFound();
}

rocksdb::Status Redis::Expire(const Slice& key, int64_t ttl_millsec) {
  std::string meta_value;
  BaseMetaKey base_meta_key(key);
  rocksdb::Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    auto type = static_cast<DataType>(static_cast<uint8_t>(meta_value[0]));
    switch (type) {
      case DataType::kSets:
        return SetsExpire(key, ttl_millsec, std::move(meta_value));
      case DataType::kZSets:
        return ZsetsExpire(key, ttl_millsec, std::move(meta_value));
      case DataType::kHashes:
        return HashesExpire(key, ttl_millsec, std::move(meta_value));
      case DataType::kLists:
        return ListsExpire(key, ttl_millsec, std::move(meta_value));
      case DataType::kStrings:
        return StringsExpire(key, ttl_millsec, std::move(meta_value));
      default:
        return rocksdb::Status::NotFound();
    }
  }
  return rocksdb::Status::NotFound();
}

rocksdb::Status Redis::Expireat(const Slice& key, int64_t timestamp_millsec) {
  std::string meta_value;
  BaseMetaKey base_meta_key(key);
  rocksdb::Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    auto type = static_cast<DataType>(static_cast<uint8_t>(meta_value[0]));
    switch (type) {
      case DataType::kSets:
        return SetsExpireat(key, timestamp_millsec, std::move(meta_value));
      case DataType::kZSets:
        return ZsetsExpireat(key, timestamp_millsec, std::move(meta_value));
      case DataType::kHashes:
        return HashesExpireat(key, timestamp_millsec, std::move(meta_value));
      case DataType::kLists:
        return ListsExpireat(key, timestamp_millsec, std::move(meta_value));
      case DataType::kStrings:
        return StringsExpireat(key, timestamp_millsec, std::move(meta_value));
      default:
        return rocksdb::Status::NotFound();
    }
  }
  return rocksdb::Status::NotFound();
}

rocksdb::Status Redis::Persist(const Slice& key) {
  std::string meta_value;
  BaseMetaKey base_meta_key(key);
  rocksdb::Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    auto type = static_cast<DataType>(static_cast<uint8_t>(meta_value[0]));
    switch (type) {
      case DataType::kSets:
        return SetsPersist(key, std::move(meta_value));
      case DataType::kZSets:
        return ZsetsPersist(key, std::move(meta_value));
      case DataType::kHashes:
        return HashesPersist(key, std::move(meta_value));
      case DataType::kLists:
        return ListsPersist(key, std::move(meta_value));
      case DataType::kStrings:
        return StringsPersist(key, std::move(meta_value));
      default:
        return rocksdb::Status::NotFound();
    }
  }
  return rocksdb::Status::NotFound();
}

rocksdb::Status Redis::TTL(const Slice& key, int64_t* ttl_millsec) {
  std::string meta_value;
  BaseMetaKey base_meta_key(key);
  rocksdb::Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    auto type = static_cast<DataType>(static_cast<uint8_t>(meta_value[0]));
    switch (type) {
      case DataType::kSets:
        return SetsTTL(key, ttl_millsec, std::move(meta_value));
      case DataType::kZSets:
        return ZsetsTTL(key, ttl_millsec, std::move(meta_value));
      case DataType::kHashes:
        return HashesTTL(key, ttl_millsec, std::move(meta_value));
      case DataType::kLists:
        return ListsTTL(key, ttl_millsec, std::move(meta_value));
      case DataType::kStrings:
        return StringsTTL(key, ttl_millsec, std::move(meta_value));
      default:
        return rocksdb::Status::NotFound();
    }
  }
  return rocksdb::Status::NotFound();
}

rocksdb::Status Redis::GetType(const storage::Slice& key, enum DataType& type) {
  std::string meta_value;
  BaseMetaKey base_meta_key(key);
  rocksdb::Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    type = static_cast<enum DataType>(static_cast<uint8_t>(meta_value[0]));
  }
  return Status::OK();
}

rocksdb::Status Redis::IsExist(const storage::Slice& key) {
  std::string meta_value;
  BaseMetaKey base_meta_key(key);
  rocksdb::Status s = db_->Get(default_read_options_, handles_[kMetaCF], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    if (ExpectedStale(meta_value)) {
      return Status::NotFound();
    }
    return Status::OK();
  }
  return rocksdb::Status::NotFound();
}

/*
 * Example Delete the specified prefix key
 */
rocksdb::Status Redis::PKPatternMatchDelWithRemoveKeys(const std::string& pattern, int64_t* ret, std::vector<std::string>* remove_keys, const int64_t& max_count) {
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  std::string key;
  std::string meta_value;
  int64_t total_delete = 0;
  rocksdb::Status s;
  rocksdb::WriteBatch batch;
  rocksdb::Iterator* iter = db_->NewIterator(iterator_options, handles_[kMetaCF]);
  iter->SeekToFirst();
  while (iter->Valid() && static_cast<int64_t>(batch.Count()) < max_count) {
    auto meta_type = static_cast<enum DataType>(static_cast<uint8_t>(iter->value()[0]));
    ParsedBaseMetaKey parsed_meta_key(iter->key().ToString());
    key = iter->key().ToString();
    meta_value = iter->value().ToString();

    if (meta_type == DataType::kStrings) {
      ParsedStringsValue parsed_strings_value(&meta_value);
      if (!parsed_strings_value.IsStale() &&
          (StringMatch(pattern.data(), pattern.size(), parsed_meta_key.Key().data(), parsed_meta_key.Key().size(), 0) != 0)) {
        batch.Delete(key);
        remove_keys->push_back(parsed_meta_key.Key().data());
      }
    } else if (meta_type == DataType::kLists) {
      ParsedListsMetaValue parsed_lists_meta_value(&meta_value);
      if (!parsed_lists_meta_value.IsStale() && (parsed_lists_meta_value.Count() != 0U) &&
          (StringMatch(pattern.data(), pattern.size(), parsed_meta_key.Key().data(), parsed_meta_key.Key().size(), 0) !=
           0)) {
        parsed_lists_meta_value.InitialMetaValue();
        batch.Put(handles_[kMetaCF], iter->key(), meta_value);
        remove_keys->push_back(parsed_meta_key.Key().data());
      }
    } else if (meta_type == DataType::kStreams) {
      StreamMetaValue stream_meta_value;
      stream_meta_value.ParseFrom(meta_value);
      if ((stream_meta_value.length() != 0) &&
          (StringMatch(pattern.data(), pattern.size(), parsed_meta_key.Key().data(), parsed_meta_key.Key().size(), 0) != 0)) {
        stream_meta_value.InitMetaValue();
        batch.Put(handles_[kMetaCF], key, stream_meta_value.value());
        remove_keys->push_back(parsed_meta_key.Key().data());
      }
    } else {
      ParsedBaseMetaValue parsed_meta_value(&meta_value);
      if (!parsed_meta_value.IsStale() && (parsed_meta_value.Count() != 0) &&
          (StringMatch(pattern.data(), pattern.size(), parsed_meta_key.Key().data(), parsed_meta_key.Key().size(), 0) !=
           0)) {
        parsed_meta_value.InitialMetaValue();
        batch.Put(handles_[kMetaCF], iter->key(), meta_value);
        remove_keys->push_back(parsed_meta_key.Key().data());
      }
    }
    iter->Next();
  }
  if (batch.Count() != 0U) {
    s = db_->Write(default_write_options_, &batch);
    if (s.ok()) {
      total_delete += static_cast<int64_t>(batch.Count());
      batch.Clear();
    } else {
      remove_keys->erase(remove_keys->end() - batch.Count(), remove_keys->end());
    }
  }

  *ret = total_delete;
  delete iter;
  return s;
}

}  //  namespace storage