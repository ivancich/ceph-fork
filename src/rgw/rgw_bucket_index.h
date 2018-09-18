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

#pragma once


#include <ostream>
#include <memory>


#include "include/buffer.h"
#include "include/encoding.h"


enum RGWBucketIndexType {
  RGWBIType_Normal = 0,
  RGWBIType_Indexless = 1,
  RGWBIType_OrderedFlex = 2,
};
std::ostream& operator<<(std::ostream& out,
			 const RGWBucketIndexType &index_type);


class RGWBucketIndexer {
public:

  static constexpr int NO_SHARD = -1;
#ifdef UNNECESSARY
  static constexpr uint32_t NUM_SHARDS_BLIND_BUCKET = UINT32_MAX;
#endif

  // using Ref = std::unique_ptr<RGWBucketIndexer>;
  using Ref = std::shared_ptr<RGWBucketIndexer>;

  virtual ~RGWBucketIndexer() {
    // empty
  }

  virtual RGWBucketIndexer* clone() const = 0;

  virtual bool has_index() const = 0;
  virtual bool is_sharded() const = 0;
  virtual int get_num_shards() const = 0;
  virtual void encode(bufferlist& bl) const = 0;
  virtual void decode_inplace(bufferlist::const_iterator& bl) = 0;
  virtual RGWBucketIndexType get_index_type() const = 0;
  
  static Ref decode(bufferlist::const_iterator& bl);
};
#if 0
WRITE_CLASS_ENCODER_UNIQPTR(RGWBucketIndexer)
#else
WRITE_CLASS_ENCODER_SHAREDPTR(RGWBucketIndexer)
#endif


class RGWBucketIndexless : public RGWBucketIndexer {
public:
  RGWBucketIndexless()
  {}

  RGWBucketIndexless* clone() const override {
    return new RGWBucketIndexless();
  }

  bool has_index() const override {
    return false;
  }

  bool is_sharded() const override {
    return false;
  }

  int get_num_shards() const override {
    return 0;
  }

  RGWBucketIndexType get_index_type() const override {
    return RGWBIType_Indexless;
  }

  void encode(bufferlist& bl) const override {
    // TODO : write out a version #?
  }

  void decode_inplace(bufferlist::const_iterator& bl) override {
    // TODO : read in a verify a version #?
  }
};


// abstract superclass for bucket indexers
class RGWBucketShardedIndexer : public RGWBucketIndexer {

protected:

  uint num_shards;

public:

  RGWBucketShardedIndexer(uint32_t _num_shards) :
    num_shards(_num_shards)
  { }

  virtual ~RGWBucketShardedIndexer()
  { }

  bool has_index() const override {
    return true;
  }

  bool is_sharded() const override {
    return true;
  }

  int get_num_shards() const override {
    return num_shards;
  }

  virtual std::string get_index_object(const std::string& objectname) = 0;

  virtual int get_shard(const std::string& bucket_oid_base,
			const std::string& obj_key,
			std::string* bucket_obj,
			int* shard_id) const = 0;
  virtual void get_bucket_index_object(const std::string& bucket_oid_base,
				       int shard_id,
				       std::string *bucket_obj) const = 0;
}; // RGWBucketShardedIndexer


class RGWBucketHashIndexer : public RGWBucketShardedIndexer {
public:
  // NOTE: is this even necessary?
  enum BIShardsHashType {
    MOD = 0
  };

protected:

  BIShardsHashType hash_type;

  RGWBucketHashIndexer(uint32_t _num_shards, BIShardsHashType _hash_type) :
    RGWBucketShardedIndexer(_num_shards),
    hash_type(_hash_type)
  { }
}; // RGWBucketHashIndexer


class RGWBucketDefaultIndexer : public RGWBucketHashIndexer {
  static constexpr int RGW_SHARDS_PRIME_0 = 7877;
  static constexpr int RGW_SHARDS_PRIME_1 = 65521;

public:
  
  RGWBucketDefaultIndexer(uint32_t _num_shards) :
    RGWBucketHashIndexer(_num_shards, MOD)
  { }

   RGWBucketDefaultIndexer* clone() const override {
    return new RGWBucketDefaultIndexer(num_shards);
  }

  // only called by rgw_shard_id and rgw_bucket_shard_index
  static int rgw_shards_mod(unsigned hval, int max_shards)
  {
    if (max_shards <= RGW_SHARDS_PRIME_0) {
      return hval % RGW_SHARDS_PRIME_0 % max_shards;
    }
    return hval % RGW_SHARDS_PRIME_1 % max_shards;
  }

  static int get_shards_max() {
    return RGW_SHARDS_PRIME_1;
  }

  static uint32_t rgw_bucket_shard_index(const std::string& key,
					 int num_shards);

  std::string get_index_object(const std::string& objectname) override {
    return "";
  }

  int get_shard(const std::string& bucket_oid_base,
		const std::string& obj_key,
		std::string* bucket_obj,
		int* shard_id) const override;

  void get_bucket_index_object(const std::string& bucket_oid_base,
			       int shard_id,
			       std::string *bucket_obj) const override;

  RGWBucketIndexType get_index_type() const override {
    return RGWBIType_Normal;
  }

  virtual void encode(bufferlist& bl) const override;
  virtual void decode_inplace(bufferlist::const_iterator& bl) override;
}; // RGWBucketDefaultIndexer
