// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2023 IBM
 *
 * See file COPYING for licensing information.
 */

#pragma once

#include <map>
#include <mutex>
#include <atomic>

#include "include/common_fwd.h"
#include "common/ceph_context.h"
#include "common/Thread.h"
#include "common/ceph_time.h"
#include "rgw_frontend.h"
#include "xxHash/xxhash.h"

#include "arrow/type.h"
#include "arrow/flight/server.h"
#include "parquet/metadata.h"

#include "rgw_flight_common.h"
#include "rgw_flight_frontend.h"


struct req_state;

namespace rgw::flight {

static const coarse_real_clock::duration lifespan = std::chrono::hours(1);


/**** FlightKey ****/

using FlightKey = XXH64_hash_t;
// Ticket and FlightKey

constexpr XXH64_hash_t flight_key_seed = 0x5475626d616e;

inline FlightKey key_from_str(const std::string& str) {
  return XXH64(str.c_str(), str.size(), flight_key_seed);
}


/**** FlightDescriptor ****/

inline flt::FlightDescriptor descriptor_from_object(const std::string& bucket,
						    const std::string& object,
						    const std::string& tenant = "") {
  return flt::FlightDescriptor::Path( {
      (tenant.empty() ? "" : tenant + ":") +
      bucket + "/" + object } );
}


/**** ObjectDescriptor ****/

struct ObjectDescriptor {
  std::string tenant;
  std::string bucket;
  std::string object;

  ObjectDescriptor(const std::string& _bucket, const std::string& _object, const std::string& _tenant = "") :
    tenant(_tenant), bucket(_bucket), object(_object)
  { }

  ObjectDescriptor(const std::string_view _bucket, const std::string_view _object, const std::string_view _tenant) :
    tenant(_tenant), bucket(_bucket), object(_object)
  { }

  rgw_bucket get_rgw_bucket() const {
    return rgw_bucket(tenant, bucket);
  }

  rgw_obj_key get_rgw_obj_key() const {
    return rgw_obj_key(object);
  }

  std::string to_string() const;

  FlightKey get_flight_key() const {
    return key_from_str(to_string());
  }

  flt::FlightDescriptor get_flight_descriptor() const {
    return descriptor_from_object(bucket, object, tenant);
  }

  static arw::Result<ObjectDescriptor> from_flight_descriptor(const flt::FlightDescriptor& d);
}; // ObjectDescriptor


inline std::ostream& operator<<(std::ostream& out, const ObjectDescriptor& od) {
  return out << od.to_string();
}


/**** FlightData ****/

struct FlightData {
  FlightKey key;
  flt::FlightDescriptor descriptor;

  std::shared_ptr<arw::Schema> schema;
  std::shared_ptr<const arw::KeyValueMetadata> kv_metadata;

  // TODO: this should be removed when we do proper flight authentication
  rgw_user user_id;

  // NB: these are not known until the query is complete; get rid of??
  uint64_t num_records;
  uint64_t obj_size;

  // in case we cannot get a FlightKey from the descriptor, throw an
  // error result, so we don't have to throw an exception
  static arw::Result<FlightData> create(const flt::FlightDescriptor&descriptor,
					std::shared_ptr<arw::Schema>&schema,
					std::shared_ptr<const arw::KeyValueMetadata>&kv_metadata,
					const rgw_user&user_id,
					uint64_t num_records = 0,
					uint64_t obj_size = 0);

private:

  FlightData(const FlightKey& key,
	     const flt::FlightDescriptor&descriptor,
	     std::shared_ptr<arw::Schema>&schema,
	     std::shared_ptr<const arw::KeyValueMetadata>&kv_metadata,
	     const rgw_user&user_id,
	     uint64_t num_records = 0,
	     uint64_t obj_size = 0);
};

// stores flights that have been created and helps expire them
class FlightStore {

protected:

  const DoutPrefix& dp;

public:

  FlightStore(const DoutPrefix& dp);
  virtual ~FlightStore();
  virtual arw::Result<FlightKey> add_flight(FlightData&& flight) = 0;

  // TODO consider returning const shared pointers to FlightData in
  // the following two functions
  virtual arw::Result<FlightData> get_flight(const FlightKey& key) const = 0;
  virtual std::optional<FlightData> after_key(const FlightKey& key) const = 0;

  virtual int remove_flight(const FlightKey& key) = 0;
  virtual int expire_flights() = 0;
};

class MemoryFlightStore : public FlightStore {
  std::map<FlightKey, FlightData> map;
  mutable std::mutex mtx; // for map

public:

  MemoryFlightStore(const DoutPrefix& dp);
  virtual ~MemoryFlightStore();
  arw::Result<FlightKey> add_flight(FlightData&& flight) override;
  arw::Result<FlightData> get_flight(const FlightKey& key) const override;
  std::optional<FlightData> after_key(const FlightKey& key) const override;
  int remove_flight(const FlightKey& key) override;
  int expire_flights() override;
};

class FlightServer : public flt::FlightServerBase {

  using Data1 = std::vector<std::shared_ptr<arw::RecordBatch>>;
  using ActionFuncPtr = arw::Status (*)(const flt::ServerCallContext& context,
					const flt::Action& action,
					std::unique_ptr<flt::ResultStream>* result);

  struct ActionRecord {
    std::string description;
    ActionFuncPtr func;
  };

  RGWProcessEnv& env;
  rgw::sal::Driver* driver;
  const DoutPrefix& dp;
  FlightStore* flight_store;

  std::map<std::string, Data1> data;
  arw::Status serve_return_value;
  std::map<std::string, ActionRecord> action_map;

public:

  static constexpr int default_port = 8077;

  FlightServer(RGWProcessEnv& env,
	       FlightStore* flight_store,
	       const DoutPrefix& dp);
  ~FlightServer() override;

  // provides a version of Serve that has no return value, to avoid
  // warnings when launching in a thread
  void ServeAlt() {
    serve_return_value = Serve();
  }

  FlightStore* get_flight_store() {
    return flight_store;
  }

  arw::Status ListFlights(const flt::ServerCallContext& context,
			  const flt::Criteria* criteria,
			  std::unique_ptr<flt::FlightListing>* listings) override;

  arw::Status GetFlightInfo(const flt::ServerCallContext &context,
			    const flt::FlightDescriptor &request,
			    std::unique_ptr<flt::FlightInfo> *info) override;

#if 0
  arw::Status PollFlightInfo(const flt::ServerCallContext& context,
			     const flt::FlightDescriptor& request,
			     std::unique_ptr<flt::PollInfo>* info) override;
#endif

  arw::Status GetSchema(const flt::ServerCallContext &context,
			const flt::FlightDescriptor &request,
			std::unique_ptr<flt::SchemaResult> *schema) override;

  arw::Status DoGet(const flt::ServerCallContext &context,
		    const flt::Ticket &request,
		    std::unique_ptr<flt::FlightDataStream> *stream) override;

  arw::Status DoPut(const flt::ServerCallContext& context,
		    std::unique_ptr<flt::FlightMessageReader> reader,
		    std::unique_ptr<flt::FlightMetadataWriter> writer) override;

  arw::Status DoExchange(const flt::ServerCallContext& context,
			 std::unique_ptr<flt::FlightMessageReader> reader,
			 std::unique_ptr<flt::FlightMessageWriter> writer) override;

  arw::Status DoAction(const flt::ServerCallContext& context,
		       const flt::Action& action,
		       std::unique_ptr<flt::ResultStream>* result) override;

  arw::Status ListActions(const flt::ServerCallContext& context,
			  std::vector<flt::ActionType>* actions) override;

  // since we're using emplace, we won't overwrite an existing entry
  // with a new one of the same key
  void add_action(const std::string& type,
		  const std::string& description,
		  ActionFuncPtr func) {
    action_map.emplace(type,
		       ActionRecord({ description, func }));
  }
}; // class FlightServer

class OwningStringView : public std::string_view {

  uint8_t* buffer;
  int64_t capacity;
  int64_t consumed;

  OwningStringView(uint8_t* _buffer, int64_t _size) :
    std::string_view((const char*) _buffer, _size),
    buffer(_buffer),
    capacity(_size),
    consumed(_size)
    { }

  OwningStringView(OwningStringView&& from, int64_t new_size) :
    buffer(nullptr),
    capacity(from.capacity),
    consumed(new_size)
    {
      // should be impossible due to static function check
      ceph_assertf(consumed <= capacity, "new size cannot exceed capacity");

      std::swap(buffer, from.buffer);
      from.capacity = 0;
      from.consumed = 0;
    }

public:

  OwningStringView(OwningStringView&&) = default;
  OwningStringView& operator=(OwningStringView&&) = default;

  uint8_t* writeable_data() {
    return buffer;
  }

  ~OwningStringView() {
    if (buffer) {
      delete[] buffer;
    }
  }

  static arw::Result<OwningStringView> make(int64_t size) {
    uint8_t* buffer = new uint8_t[size];
    if (!buffer) {
      return arw::Status::OutOfMemory("could not allocated buffer of size %" PRId64, size);
    }
    return OwningStringView(buffer, size);
  }

  static arw::Result<OwningStringView> shrink(OwningStringView&& from,
					      int64_t new_size) {
    if (new_size > from.capacity) {
      return arw::Status::Invalid("new size cannot exceed capacity");
    } else {
      return OwningStringView(std::move(from), new_size);
    }
  }

};

// GLOBAL

flt::Ticket FlightKeyToTicket(const FlightKey& key);
arw::Status TicketToFlightKey(const flt::Ticket& t, FlightKey& key);
arw::Status process_pq_metadata(const std::shared_ptr<arw::io::RandomAccessFile>& reader,
				std::shared_ptr<const arw::KeyValueMetadata>& kv_metadata,
				std::shared_ptr<arw::Schema>& aw_schema,
				int64_t& num_rows,
				const DoutPrefix& dp,
				const std::string& description = "");

} // namespace rgw::flight
