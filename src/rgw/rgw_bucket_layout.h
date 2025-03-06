// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

/* N.B., this header defines fundamental serialized types.  Do not
 * introduce changes or include files which can only be compiled in
 * radosgw or OSD contexts (e.g., rgw_sal.h, rgw_common.h)
 */

#pragma once

#include <string>
#include <sstream>
#include <optional>

#include "include/encoding.h"
#include "common/ceph_json.h"


class RGWBucketInfo;

namespace rgw {

enum class BucketIndexType : uint8_t {
  Normal = 0,    // normal hash-based sharded index layout
  Indexless = 1, // no bucket index, so listing is unsupported
  Ordered = 2,   // shards maintain lexical order
};

std::string_view to_string(const BucketIndexType& t);
bool parse(std::string_view str, BucketIndexType& t);
void encode_json_impl(const char *name, const BucketIndexType& t, ceph::Formatter *f);
void decode_json_obj(BucketIndexType& t, JSONObj *obj);

inline std::ostream& operator<<(std::ostream& out, const BucketIndexType& t)
{
  return out << to_string(t);
}

enum class BucketHashType : uint8_t {
  Mod, // rjenkins hash of object name, modulo num_shards
};

std::string_view to_string(const BucketHashType& t);
bool parse(std::string_view str, BucketHashType& t);
void encode_json_impl(const char *name, const BucketHashType& t, ceph::Formatter *f);
void decode_json_obj(BucketHashType& t, JSONObj *obj);



struct bucket_index_normal_layout {
  uint32_t num_shards = 1;

  // the fewest number of shards this bucket layout allows
  uint32_t min_num_shards = 1;

  BucketHashType hash_type = BucketHashType::Mod;

  std::string to_string() const {
    std::stringstream ss;
    ss << "bucket_index_hashed_layout{ num_shards=" << num_shards <<
      ", min_num_shards=" << min_num_shards <<
      ", hash_type=" << rgw::to_string(hash_type) << " }";
    return ss.str();
  }

  friend std::ostream& operator<<(std::ostream& out, const bucket_index_normal_layout& l) {
    out << l.to_string();
    return out;
  }
}; // struct bucket_index_normal_layout

inline bool operator==(const bucket_index_normal_layout& l,
                       const bucket_index_normal_layout& r) {
  return l.num_shards == r.num_shards
      && l.hash_type == r.hash_type;
}
inline bool operator!=(const bucket_index_normal_layout& l,
                       const bucket_index_normal_layout& r) {
  return !(l == r);
}

void encode(const bucket_index_normal_layout& l, bufferlist& bl, uint64_t f=0);
void decode(bucket_index_normal_layout& l, bufferlist::const_iterator& bl);
void encode_json_impl(const char *name, const bucket_index_normal_layout& l, ceph::Formatter *f);
void decode_json_obj(bucket_index_normal_layout& l, JSONObj *obj);

struct bucket_index_ordered_layout {
  uint32_t num_shards = 1;

  std::string to_string() const {
    std::stringstream ss;
    ss << "bucket_index_ordered_layout:{ num_shards=" << num_shards << " }";
    return ss.str();
  }

  friend std::ostream& operator<<(std::ostream& out, const bucket_index_ordered_layout& l) {
    out << l.to_string();
    return out;
  }
}; // struct bucket_index_ordered_layout

inline bool operator==(const bucket_index_ordered_layout& l,
                       const bucket_index_ordered_layout& r) {
  return l.num_shards == r.num_shards;
}

inline bool operator!=(const bucket_index_ordered_layout& l,
                       const bucket_index_ordered_layout& r) {
  return !(l == r);
}

void encode(const bucket_index_ordered_layout& l, bufferlist& bl, uint64_t f=0);
void decode(bucket_index_ordered_layout& l, bufferlist::const_iterator& bl);
void encode_json_impl(const char *name, const bucket_index_ordered_layout& l, ceph::Formatter *f);
void decode_json_obj(bucket_index_ordered_layout& l, JSONObj *obj);


using LayoutVariant = std::variant<bucket_index_normal_layout,bucket_index_ordered_layout>;

BucketIndexType bucket_index_type(const LayoutVariant& l);


struct bucket_index_layout {
  BucketIndexType type;

  LayoutVariant normal;

  friend std::ostream& operator<<(std::ostream& out, const bucket_index_layout& l);
}; // struct bucket_index_layout

inline bool operator==(const bucket_index_layout& l,
                       const bucket_index_layout& r) {
  return l.type == r.type && l.normal == r.normal;
}
inline bool operator!=(const bucket_index_layout& l,
                       const bucket_index_layout& r) {
  return !(l == r);
}

void encode(const bucket_index_layout& l, bufferlist& bl, uint64_t f=0);
void decode(bucket_index_layout& l, bufferlist::const_iterator& bl);
void encode_json_impl(const char *name, const bucket_index_layout& l, ceph::Formatter *f);
void decode_json_obj(bucket_index_layout& l, JSONObj *obj);

struct bucket_index_layout_generation {
  uint64_t gen = 0;
  bucket_index_layout layout;

  friend std::ostream& operator<<(std::ostream& out, const bucket_index_layout_generation& g) {
    out << "bucket_index_layout_generation{ gen=" << g.gen << ", layout=" << g.layout << " }";
    return out;
  }
};

inline bool operator==(const bucket_index_layout_generation& l,
                       const bucket_index_layout_generation& r) {
  return l.gen == r.gen && l.layout == r.layout;
}
inline bool operator!=(const bucket_index_layout_generation& l,
                       const bucket_index_layout_generation& r) {
  return !(l == r);
}

void encode(const bucket_index_layout_generation& l, bufferlist& bl, uint64_t f=0);
void decode(bucket_index_layout_generation& l, bufferlist::const_iterator& bl);
void encode_json_impl(const char *name, const bucket_index_layout_generation& l, ceph::Formatter *f);
void decode_json_obj(bucket_index_layout_generation& l, JSONObj *obj);


enum class BucketLogType : uint8_t {
  // colocated with bucket index, so the log layout matches the index layout
  InIndex,
  Deleted
};

std::string_view to_string(const BucketLogType& t);
bool parse(std::string_view str, BucketLogType& t);
void encode_json_impl(const char *name, const BucketLogType& t, ceph::Formatter *f);
void decode_json_obj(BucketLogType& t, JSONObj *obj);

inline std::ostream& operator<<(std::ostream& out, const BucketLogType &log_type)
{
  switch (log_type) {
    case BucketLogType::InIndex:
      return out << "InIndex";
    case BucketLogType::Deleted:
      return out << "Deleted";
    default:
      return out << "Unknown";
  }
}

struct bucket_index_log_layout {
  uint64_t gen = 0;

  LayoutVariant layout;

  operator bucket_index_layout_generation() const {
    bucket_index_layout_generation bilg;
    bilg.gen = gen;
    bilg.layout.type = bucket_index_type(bilg.layout.normal);
    bilg.layout.normal = layout;
    return bilg;
  }
};

void encode(const bucket_index_log_layout& l, bufferlist& bl, uint64_t f=0);
void decode(bucket_index_log_layout& l, bufferlist::const_iterator& bl);
void encode_json_impl(const char *name, const bucket_index_log_layout& l, ceph::Formatter *f);
void decode_json_obj(bucket_index_log_layout& l, JSONObj *obj);

struct bucket_log_layout {
  BucketLogType type = BucketLogType::InIndex;

  bucket_index_log_layout in_index;

  friend std::ostream& operator<<(std::ostream& out, const bucket_log_layout& l) {
    out << "type=" << to_string(l.type);
    return out;
  }
};

void encode(const bucket_log_layout& l, bufferlist& bl, uint64_t f=0);
void decode(bucket_log_layout& l, bufferlist::const_iterator& bl);
void encode_json_impl(const char *name, const bucket_log_layout& l, ceph::Formatter *f);
void decode_json_obj(bucket_log_layout& l, JSONObj *obj);

struct bucket_log_layout_generation {
  uint64_t gen = 0;
  bucket_log_layout layout;

  friend std::ostream& operator<<(std::ostream& out, const bucket_log_layout_generation& g) {
    out << "gen=" << g.gen << ", layout=[ " << g.layout << " ]";
    return out;
  }
};

void encode(const bucket_log_layout_generation& l, bufferlist& bl, uint64_t f=0);
void decode(bucket_log_layout_generation& l, bufferlist::const_iterator& bl);
void encode_json_impl(const char *name, const bucket_log_layout_generation& l, ceph::Formatter *f);
void decode_json_obj(bucket_log_layout_generation& l, JSONObj *obj);

// return a log layout that shares its layout with the index
inline bucket_log_layout_generation log_layout_from_index(
    uint64_t gen, const bucket_index_layout_generation& index)
{
  return { gen, { BucketLogType::InIndex, { index.gen, index.layout.normal }}};
}

inline auto matches_gen(uint64_t gen)
{
  return [gen] (const bucket_log_layout_generation& l) { return l.gen == gen; };
}

inline bucket_index_layout_generation log_to_index_layout(const bucket_log_layout_generation& log_layout)
{
  bucket_index_layout_generation index;
  index.gen = log_layout.layout.in_index.gen;
  index.layout.normal = log_layout.layout.in_index.layout;
  return index;
}

enum class BucketReshardState : uint8_t {
  None,
  InProgress,
  InLogrecord,
};
std::string_view to_string(const BucketReshardState& s);
bool parse(std::string_view str, BucketReshardState& s);
void encode_json_impl(const char *name, const BucketReshardState& s, ceph::Formatter *f);
void decode_json_obj(BucketReshardState& s, JSONObj *obj);

// describes the layout of bucket index objects
struct BucketLayout {
  BucketReshardState resharding = BucketReshardState::None;

  // current bucket index layout
  bucket_index_layout_generation current_index;

  // target index layout of a resharding operation
  std::optional<bucket_index_layout_generation> target_index;

  // history of untrimmed bucket log layout generations, with the current
  // generation at the back()
  std::vector<bucket_log_layout_generation> logs;

  // via this time to judge if the bucket is resharding, when the reshard status
  // of bucket changed or the reshard status is read, this time will be updated
  ceph::real_time judge_reshard_lock_time;

  friend std::ostream& operator<<(std::ostream& out, const BucketLayout& l) {
    std::stringstream ss;
    if (l.target_index) {
      ss << *l.target_index;
    } else {
      ss << "none";
    }
    out << "resharding=" << to_string(l.resharding) <<
      ", current_index=[" << l.current_index << "], target_index=[" <<
      ss.str() << "], logs.size()=" << l.logs.size() <<
      ", judge_reshard_lock_time=" << l.judge_reshard_lock_time;

    return out;
  }
};

void encode(const BucketLayout& l, bufferlist& bl, uint64_t f=0);
void decode(BucketLayout& l, bufferlist::const_iterator& bl);
void encode_json_impl(const char *name, const BucketLayout& l, ceph::Formatter *f);
void decode_json_obj(BucketLayout& l, JSONObj *obj);


inline uint32_t num_shards(const bucket_index_normal_layout& index) {
  // old buckets used num_shards=0 to mean 1
  return index.num_shards > 0 ? index.num_shards : 1;
}
inline uint32_t num_shards(const bucket_index_ordered_layout& index) {
  return index.num_shards;
}
inline uint32_t num_shards(const LayoutVariant& index) {
  return std::visit([](const auto& arg) -> uint32_t { return num_shards(arg); }, index);
}
inline uint32_t num_shards(const bucket_index_layout& index) {
  ceph_assert(index.type == BucketIndexType::Normal);
  return num_shards(index.normal);
}
inline uint32_t num_shards(const bucket_index_layout_generation& index) {
  return num_shards(index.layout);
}

inline uint32_t current_num_shards(const BucketLayout& layout) {
  return num_shards(layout.current_index);
}
inline bool is_layout_indexless(const bucket_index_layout_generation& layout) {
  return layout.layout.type == BucketIndexType::Indexless;
}
inline bool is_layout_reshardable(const bucket_index_layout_generation& layout) {
  return layout.layout.type == BucketIndexType::Normal ||
    layout.layout.type == BucketIndexType::Ordered;
}
inline bool is_layout_reshardable(const BucketLayout& layout) {
  return is_layout_reshardable(layout.current_index);
}
inline std::string_view current_layout_desc(const BucketLayout& layout) {
  return rgw::to_string(layout.current_index.layout.type);
}
inline bucket_index_normal_layout* hashed_layout_ptr(bucket_index_layout& layout) {
  return std::get_if<rgw::bucket_index_normal_layout>(&layout.normal);
}
inline const bucket_index_normal_layout* hashed_layout_ptr(const bucket_index_layout& layout) {
  return std::get_if<rgw::bucket_index_normal_layout>(&layout.normal);
}
inline bucket_index_ordered_layout* ordered_layout_ptr(bucket_index_layout& layout) {
  return std::get_if<rgw::bucket_index_ordered_layout>(&layout.normal);
}
inline const bucket_index_ordered_layout* ordered_layout_ptr(const bucket_index_layout& layout) {
  return std::get_if<rgw::bucket_index_ordered_layout>(&layout.normal);
}

} // namespace rgw
