// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2023 IBM
 *
 * See file COPYING for licensing information.
 */

#include <iostream>
#include <fstream>
#include <sstream>
#include <mutex>
#include <map>
#include <algorithm>
#include <type_traits>

#include "arrow/type.h"
#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/ipc/reader.h"
#include "arrow/table.h"

#include "arrow/flight/server.h"

#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"

#include "common/dout.h"
#include "rgw_op.h"

#include "rgw_flight.h"
#include "rgw_flight_frontend.h"
#include "rgw_flight_random_access.h"


namespace rgw::flight {


std::string ObjectDescriptor::to_string() const {
  if (tenant.empty()) {
    return bucket + "/" + object;
  } else {
    return tenant + ":" + bucket + "/" + object;
  }
}

arw::Result<ObjectDescriptor> ObjectDescriptor::from_flight_descriptor(const flt::FlightDescriptor& d) {

  if (d.type != flt::FlightDescriptor::DescriptorType::PATH) {
    return arw::Status::Invalid("did not provide PATH type FlightDescriptor");
  }

  std::string_view tenant_str;
  std::string_view bucket_str;
  std::stringstream object_ss;

  auto i = d.path.cbegin();
  if (i == d.path.cend()) {
    return arw::Status::Invalid("path provided is invalid; no bucket");
  }

  auto colon = i->find_first_of(":");
  auto slash = i->find_first_of("/");
  if (colon != std::string::npos) {
    tenant_str = std::string_view(i->data(), colon);
    if (slash != std::string::npos) {
      if (slash < colon) {
	return arw::Status::Invalid("colon delimiter (for tenant) appears after slash delimiter (for path)");
      }
      bucket_str = std::string_view(1 + colon + i->data(), slash - colon - 1);
      object_ss << (slash + i->data());
    } else {
      bucket_str = std::string_view(1 + colon + i->data());
    }
  } else {
    if (slash != std::string::npos) {
      bucket_str = std::string_view(i->data(), slash);
      object_ss << std::string_view(1 + slash + i->data());
    } else {
      bucket_str = *i;
    }
  }

  while (++i != d.path.cend()) {
    object_ss << "/" << *i;
  }

  const std::string object_str = object_ss.str();
  const std::string_view object_sv =
    (!object_str.empty() && object_str[0] == '/') ?
    object_str.data() + 1 :
    object_str.data();

  if (object_sv.empty()) {
    return arw::Status::Invalid("no object provided");
  }

  return ObjectDescriptor(bucket_str, object_sv, tenant_str);
} // from_flight_descriptor()


arw::Result<FlightKey> key_from_descriptor(const flt::FlightDescriptor& d) {
  switch (d.type) {
  case flt::FlightDescriptor::DescriptorType::PATH:
  {
    ARROW_ASSIGN_OR_RAISE(ObjectDescriptor desc, ObjectDescriptor::from_flight_descriptor(d));
    return desc.get_flight_key();
  }
  case flt::FlightDescriptor::DescriptorType::CMD:
    return key_from_str(d.cmd);

  default:
    return arw::Status::Invalid("did not understand FlightDescriptor provided");
  }
}

flt::Ticket FlightKeyToTicket(const FlightKey& key) {
  flt::Ticket result;
  result.ticket = std::to_string(key);
  return result;
}

arw::Result<FlightKey> TicketToFlightKey(const flt::Ticket& t) {
  try {
    return (FlightKey) std::stoul(t.ticket);
  } catch (std::invalid_argument const& ex) {
    return arw::Status::Invalid(
      "could not convert Ticket containing \"%s\" into a Flight Key",
      t.ticket);
  } catch (const std::out_of_range& ex) {
    return arw::Status::Invalid(
      "could not convert Ticket containing \"%s\" into a Flight Key due to range",
      t.ticket);
  }
}

// Endpoints

std::vector<flt::FlightEndpoint> make_endpoints(const FlightKey& key) {
  flt::FlightEndpoint endpoint;
  endpoint.ticket = FlightKeyToTicket(key);
  return { endpoint };
  // return std::vector<flt::FlightEndpoint> endpoints { endpoint };
}

arw::Result<std::unique_ptr<flt::FlightInfo>>
make_flight_info(const FlightKey& key,
		 const flt::FlightDescriptor& d,
		 const FlightData& fd) {
  ARROW_ASSIGN_OR_RAISE(flt::FlightInfo info_obj,
			flt::FlightInfo::Make(*fd.schema,
					      d,
					      make_endpoints(fd.key),
					      fd.num_records,
					      fd.obj_size));
  return std::make_unique<flt::FlightInfo>(std::move(info_obj));
}


// FlightData

FlightData::FlightData(const FlightKey& _key,
		       const flt::FlightDescriptor& _descriptor,
		       std::shared_ptr<arw::Schema>& _schema,
		       std::shared_ptr<const arw::KeyValueMetadata>& _kv_metadata,
		       const rgw_user& _user_id,
		       uint64_t _num_records,
		       uint64_t _obj_size) :
  key(_key),
  descriptor(_descriptor),
  schema(_schema),
  kv_metadata(_kv_metadata),
  user_id(_user_id),
  num_records(_num_records),
  obj_size(_obj_size)
{ }


arw::Result<FlightData> FlightData::create(const flt::FlightDescriptor& descriptor,
					   std::shared_ptr<arw::Schema>& schema,
					   std::shared_ptr<const arw::KeyValueMetadata>& kv_metadata,
					   const rgw_user& user_id,
					   uint64_t num_records,
					   uint64_t obj_size) {
  ARROW_ASSIGN_OR_RAISE(FlightKey key, key_from_descriptor(descriptor));
  return FlightData(key, descriptor, schema, kv_metadata, user_id, num_records, obj_size);
}


/**** FlightStore ****/

FlightStore::FlightStore(const DoutPrefix& _dp) :
  dp(_dp)
{ }

FlightStore::~FlightStore() { }

/**** MemoryFlightStore ****/

MemoryFlightStore::MemoryFlightStore(const DoutPrefix& _dp) :
  FlightStore(_dp)
{ }

MemoryFlightStore::~MemoryFlightStore() { }

arw::Result<FlightKey> MemoryFlightStore::add_flight(FlightData&& flight) {
  std::pair<decltype(map)::iterator,bool> result;
  {
    const std::lock_guard lock(mtx);
    result = map.insert({ flight.key, std::move(flight) });
  }
  if (!result.second) {
    return arw::Status::IOError("unable to add flight with key %" PRIu64,
				flight.key);
  }

  return flight.key;
}

arw::Result<FlightData> MemoryFlightStore::get_flight(const FlightKey& key) const {
  const std::lock_guard lock(mtx);
  auto i = map.find(key);
  if (i == map.cend()) {
    return arw::Status::KeyError("could not find flight with key %" PRIu64,
				 key);
  } else {
    return i->second;
  }
}

// returns either the next FilghtData or, if at end, empty optional
std::optional<FlightData> MemoryFlightStore::after_key(const FlightKey& key) const {
  std::optional<FlightData> result;
  {
    const std::lock_guard lock(mtx);
    auto i = map.upper_bound(key);
    if (i != map.end()) {
      result = i->second;
    }
  }
  return result;
}

int MemoryFlightStore::remove_flight(const FlightKey& key) {
  return 0;
}

int MemoryFlightStore::expire_flights() {
  return 0;
}

/**** FlightServer ****/

  /** Actions **/

  arw::Status HealthCheckAction(const flt::ServerCallContext& context,
				const flt::Action& action,
				std::unique_ptr<flt::ResultStream>* result) {
    *result = std::unique_ptr<arrow::flight::ResultStream>(
      new arrow::flight::SimpleResultStream({}));

    return arw::Status::OK();
  }


  /** Implementation **/

FlightServer::FlightServer(RGWProcessEnv& _env,
			   FlightStore* _flight_store,
			   const DoutPrefix& _dp) :
  env(_env),
  driver(env.driver),
  dp(_dp),
  flight_store(_flight_store)
{
  add_action("healthcheck",
	     "evaluate the health of the arrow flight server",
	     HealthCheckAction);
}

FlightServer::~FlightServer()
{ }


arw::Status FlightServer::ListFlights(const flt::ServerCallContext& context,
				      const flt::Criteria* criteria,
				      std::unique_ptr<flt::FlightListing>* listings) {
  FINFO << "entered" << dendl;
  // function local class to implement FlightListing interface
  class RGWFlightListing : public flt::FlightListing {

    FlightStore* flight_store;
    FlightKey previous_key;

  public:

    RGWFlightListing(FlightStore* flight_store) :
      flight_store(flight_store),
      previous_key(0)
      { }

    arrow::Result<std::unique_ptr<flt::FlightInfo>> Next() override {
      std::optional<FlightData> fd = flight_store->after_key(previous_key);
      if (fd) {
	previous_key = fd->key;
	return make_flight_info(fd->key, fd->descriptor, *fd);
      } else {
	return nullptr;
      }
    }
  }; // class RGWFlightListing

  *listings = std::make_unique<RGWFlightListing>(flight_store);
  return arw::Status::OK();
} // FlightServer::ListFlights


static arw::Result<FlightData> create_flight_from_path(rgw::sal::Driver* driver,
						       FlightStore* flight_store,
						       const flt::FlightDescriptor& descriptor,
						       const DoutPrefix& dp)
{
  int ret;

  std::shared_ptr<const arw::KeyValueMetadata> kv_metadata;
  std::shared_ptr<arw::Schema> aw_schema;
  int64_t num_rows = 0;

  ARROW_ASSIGN_OR_RAISE(ObjectDescriptor od, ObjectDescriptor::from_flight_descriptor(descriptor));

  std::unique_ptr<rgw::sal::Bucket> sal_bucket;
  ret = driver->load_bucket(&dp, od.get_rgw_bucket(), &sal_bucket, null_yield);
  if (ret) {
    FERROR << "unable to load bucket for object " << od << dendl;
    return arw::Status::IOError("unable to load bucket for %s",
				od.to_string().c_str());
  }

  auto sal_obj = sal_bucket->get_object(od.get_rgw_obj_key());
  if (! sal_obj) {
    FERROR << "unable to get object from bucket " << od << dendl;
    return arw::Status::IOError("unable to retrieve object from bucket for %s",
				od.to_string().c_str());
  }

  ret = sal_obj->load_obj_state(&dp, null_yield);
  if (ret) {
    FERROR << "unable to load object state " << od << dendl;
    return arw::Status::IOError("unable to load object state for %s",
				od.to_string().c_str());
  }

  uint64_t obj_size = sal_obj->get_size();
  auto readable_obj = std::make_shared<RandomAccessObject>(dp, sal_obj);

  ARROW_RETURN_NOT_OK(readable_obj->Open());

  auto metadata_result = process_pq_metadata(readable_obj, kv_metadata, aw_schema, num_rows, dp, descriptor.ToString());

  // if there's an issue with closing, we'll push log and push forward
  auto close_status = readable_obj->Close();
  if (! close_status.ok()) {
    FWARN << "unable to cleanly close object after metadata scan; object:" <<
      od << ", error:" << close_status << dendl;
  }

  if (! metadata_result.ok()) {
    FERROR << "unable to read metadata for " << descriptor.ToString() << dendl;
    return metadata_result;
  }

  ARROW_ASSIGN_OR_RAISE(FlightData flight_data,
			FlightData::create(descriptor,
					    aw_schema, kv_metadata,
					   rgw_user(), // NB: figure this out later
					   num_rows, obj_size));
  ARROW_ASSIGN_OR_RAISE(FlightKey key, flight_store->add_flight(std::move(flight_data)));
  FINFO << "flight added to store with key: " << key << dendl;

  return flight_data;
} // create_flight_from_path()


arw::Status FlightServer::GetFlightInfo(const flt::ServerCallContext& context,
					const flt::FlightDescriptor& request,
					std::unique_ptr<flt::FlightInfo>* info) {
  auto describe = [&request] () -> std::string {
    std::stringstream ss;
    if (request.type == flt::FlightDescriptor::DescriptorType::PATH) {
      ss << "path: \"";
      bool first = true;
      for (const auto& p : request.path) {
	ss << (first ? "" : "/") << p;
	first = false;
      }
      ss << "\"";
    } else if (request.type == flt::FlightDescriptor::DescriptorType::CMD) {
      ss << "command: \"" << request.cmd << "\"";
    } else {
      ss << "unknown request type " << request.type;
    }
    return ss.str();
  };
  FINFO << "entered: " << describe() << dendl;

  ARROW_ASSIGN_OR_RAISE(const auto key,
			key_from_descriptor(request));

  auto r = flight_store->get_flight(key);
  if (r.ok()) {
    FINFO << "found flight for key " << key << dendl;
    const FlightData& fd = r.ValueOrDie();
    ARROW_ASSIGN_OR_RAISE(*info,
			  make_flight_info(key, request, fd));
    return arw::Status::OK();
  } if (r.status().IsKeyError()) {
    FWARN << "request for key " << key << " needs to be generated" << dendl;
    if (request.type == flt::FlightDescriptor::DescriptorType::PATH) {
      ARROW_ASSIGN_OR_RAISE(FlightData flight_data,
			    create_flight_from_path(driver, flight_store, request, dp));

      // extract flight info
      return arw::Status::OK();
    } else if (request.type == flt::FlightDescriptor::DescriptorType::CMD) {
      return arw::Status::NotImplemented("generating flight from command not yet implemented");
    } else {
      return arw::Status::Invalid("do not understand FlightDescriptor type %d", request.type);
    }
  } else {
    return r.status();
  }
} // FlightServer::GetFlightInfo


arw::Status FlightServer::GetSchema(const flt::ServerCallContext &context,
				    const flt::FlightDescriptor &request,
				    std::unique_ptr<flt::SchemaResult> *schema) {
  FWARN << "NOT IMPLEMENTED" << dendl;
  return arw::Status::NotImplemented("Not Implemented");
} // FlightServer::GetSchema


arw::Status FlightServer::DoGet(const flt::ServerCallContext &context,
				const flt::Ticket &request,
				std::unique_ptr<flt::FlightDataStream> *stream) {
  int ret;

  ARROW_ASSIGN_OR_RAISE(FlightKey key, TicketToFlightKey(request));
  ARROW_ASSIGN_OR_RAISE(FlightData fd, get_flight_store()->get_flight(key));

#if 0
  /* load_bucket no longer requires a user parameter. Keep this code
   * around a bit longer until we fully figure out how permissions
   * will impact this code.
   */
  std::unique_ptr<rgw::sal::User> user = driver->get_user(fd.user_id);
  if (user->empty()) {
    FINFO << "user is empty" << dendl;
  } else {
    // TODO: test what happens if user is not loaded
    ret = user->load_user(&dp, null_yield);
    if (ret < 0) {
      FERROR << "load_user returned " << ret << dendl;
      // TODO return something
    }
    FINFO << "user is " << user->get_display_name() << dendl;
  }
#endif

  ARROW_ASSIGN_OR_RAISE(ObjectDescriptor obj_desc,
			ObjectDescriptor::from_flight_descriptor(fd.descriptor));

  std::unique_ptr<rgw::sal::Bucket> bucket;
  ret = driver->load_bucket(&dp,
			    obj_desc.get_rgw_bucket(),
                            &bucket, null_yield);
  if (ret < 0) {
    FERROR << "get_bucket returned " << ret << dendl;
    // TODO return something
  }

  std::unique_ptr<rgw::sal::Object> object = bucket->get_object(obj_desc.get_rgw_obj_key());

  auto input = std::make_shared<RandomAccessObject>(dp, object);
  ARROW_RETURN_NOT_OK(input->Open());

  std::unique_ptr<parquet::arrow::FileReader> reader;
  ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input,
					       arw::default_memory_pool(),
					       &reader));

  std::shared_ptr<arrow::Table> table;
  ARROW_RETURN_NOT_OK(reader->ReadTable(&table));

  std::vector<std::shared_ptr<arw::RecordBatch>> batches;
  arw::TableBatchReader batch_reader(*table);
  while (true) {
    std::shared_ptr<arw::RecordBatch> p;
    auto s = batch_reader.ReadNext(&p);
    if (!s.ok()) {
      break;
    }
    batches.push_back(p);
  }

  ARROW_ASSIGN_OR_RAISE(auto owning_reader,
			arw::RecordBatchReader::Make(
			  std::move(batches), table->schema()));
  *stream = std::unique_ptr<flt::FlightDataStream>(
    new flt::RecordBatchStream(owning_reader));

  return arw::Status::OK();
} // flightServer::DoGet

arw::Status FlightServer::DoPut(const flt::ServerCallContext& context,
				std::unique_ptr<flt::FlightMessageReader> reader,
				std::unique_ptr<flt::FlightMetadataWriter> writer) {
  FWARN << "NOT IMPLEMENTED" << dendl;
  return arw::Status::NotImplemented("Not Implemented");
}

arw::Status FlightServer::DoExchange(const flt::ServerCallContext& context,
				     std::unique_ptr<flt::FlightMessageReader> reader,
				     std::unique_ptr<flt::FlightMessageWriter> writer) {
  FWARN << "NOT IMPLEMENTED" << dendl;
  return arw::Status::NotImplemented("Not Implemented");
}

arw::Status FlightServer::DoAction(const flt::ServerCallContext& context,
				   const flt::Action& action,
				   std::unique_ptr<flt::ResultStream>* result) {
  auto i = action_map.find(action.type);
  if (i == action_map.end()) {
    FWARN << "action " << action.type << " NOT IMPLEMENTED" << dendl;
    return arw::Status::NotImplemented(
      "action " + action.type + " not implemented");
//      std::string("action ") + action.type + " not implemented");
  }

  FINFO << "executing action " << action.type << dendl;
  return i->second.func(context, action, result);
}

arw::Status FlightServer::ListActions(const flt::ServerCallContext& context,
				      std::vector<flt::ActionType>* actions) {
  FINFO << "entered" << dendl;
  for (const auto& i : action_map) {
    actions->push_back(flt::ActionType({ i.first, i.second.description }));
  }
  return arw::Status::OK();
}


arw::Status process_pq_metadata(const std::shared_ptr<arw::io::RandomAccessFile>& reader,
				std::shared_ptr<const arw::KeyValueMetadata>& kv_metadata,
				std::shared_ptr<arw::Schema>& aw_schema,
				int64_t& num_rows,
				const DoutPrefix& dp,
				const std::string& description) {
  try {
    const std::shared_ptr<parquet::FileMetaData> metadata = parquet::ReadMetaData(reader);

    if (metadata) {
      num_rows = metadata->num_rows();
      kv_metadata = metadata->key_value_metadata();
      const parquet::SchemaDescriptor* pq_schema = metadata->schema();
      ARROW_RETURN_NOT_OK(parquet::arrow::FromParquetSchema(pq_schema, &aw_schema));
      return arrow::Status::OK();
    } else {
      const std::string error = "unable to read metadata" +
	(description.empty() ? "" : " for " + description);
      FERROR << error << dendl;
      return arw::Status::IOError(error);
    }
  } catch (const parquet::ParquetException& e) {
    std::stringstream ss;
    ss << "unable to read metadata due to " << e.what();
    if (!description.empty()) {
      ss << " for " << description;
    }
    FERROR << ss.str() << dendl;
    return arw::Status::IOError(ss.str());
  }
}

} // namespace rgw::flight
