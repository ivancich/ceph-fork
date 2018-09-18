// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 * Copyright (C) 2015 Yehuda Sadeh <yehuda@redhat.com>
 * Copyright (C) 2018 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include <ostream>

#include "rgw_bucket_index.h"
#include "include/ceph_hash.h"


std::ostream& operator<<(std::ostream& out,
                         const RGWBucketIndexType &index_type) {
  switch (index_type) {
    case RGWBIType_Normal:
      return out << "Normal";
    case RGWBIType_Indexless:
      return out << "Indexless";
    case RGWBIType_OrderedFlex:
      return out << "OrderedFlex";
    default:
      return out << "Unknown";
  }
}


uint32_t RGWBucketDefaultIndexer::rgw_bucket_shard_index(
    const std::string& key,
    int num_shards) {
  uint32_t sid = ceph_str_hash_linux(key.c_str(), key.size());
  uint32_t sid2 = sid ^ ((sid & 0xFF) << 24);
  return rgw_shards_mod(sid2, num_shards);
}


int RGWBucketDefaultIndexer::get_shard(
    const std::string& bucket_oid_base,
    const std::string& obj_key,
    std::string* bucket_obj,
    int* shard_id) const {
  if (!num_shards) {
    // By default with no sharding, we use the bucket oid as itself
    (*bucket_obj) = bucket_oid_base;
    if (shard_id) {
      *shard_id = -1;
    }
  } else {
    uint32_t sid = rgw_bucket_shard_index(obj_key, num_shards);
    char buf[bucket_oid_base.size() + 32];
    snprintf(buf, sizeof(buf), "%s.%d", bucket_oid_base.c_str(), sid);
    (*bucket_obj) = buf;
    if (shard_id) {
      *shard_id = (int)sid;
    }
  }
  return 0;
}


void RGWBucketDefaultIndexer::get_bucket_index_object(
    const std::string& bucket_oid_base,
    int shard_id,
    std::string *bucket_obj) const {
  if (!num_shards) {
    // By default with no sharding, we use the bucket oid as itself
    (*bucket_obj) = bucket_oid_base;
  } else {
    char buf[bucket_oid_base.size() + 32];
    snprintf(buf, sizeof(buf), "%s.%d", bucket_oid_base.c_str(), shard_id);
    (*bucket_obj) = buf;
  }
}


#if 0
RGWBucketIndexer::Ref decode(bufferlist::const_iterator& bl) {
#warning implement this
  return std::unique_ptr<RGWBucketIndexer>();
}
#else
RGWBucketIndexer::Ref decode(bufferlist::const_iterator& bl) {
#warning implement this
  return std::shared_ptr<RGWBucketIndexer>();
}
#endif


void RGWBucketDefaultIndexer::encode(bufferlist& bl) const {
#warning implement this
  // empty for now
}

void RGWBucketDefaultIndexer::decode_inplace(bufferlist::const_iterator& bl) {
#warning implement this
  // empty for now
}
