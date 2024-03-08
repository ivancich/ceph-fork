// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2023 IBM
 *
 * See file COPYING for licensing information.
 */

#include <cstdio>
#include <filesystem>
#include <sstream>

#include "arrow/type.h"
#include "arrow/flight/server.h"
#include "arrow/io/file.h"

#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/stream_reader.h"

#include "rgw_flight_common.h"
#include "rgw_flight_frontend.h"
#include "rgw_flight_random_access.h"
#include "rgw_flight.h"


// logging
constexpr unsigned dout_subsys = ceph_subsys_rgw_flight;
constexpr const char* dout_prefix_str = "rgw arrow_flight: ";


namespace rgw::flight {

FlightFrontend::FlightFrontend(RGWProcessEnv& _env,
			       RGWFrontendConfig* _config,
			       int _port) :
  env(_env),
  config(_config),
  port(_port),
  dp(env.driver->ctx(), dout_subsys, dout_prefix_str)
{
  env.flight_store = new MemoryFlightStore(dp);
  env.flight_server = new FlightServer(env, env.flight_store, dp);
  FINFO << "flight server started" << dendl;
}

FlightFrontend::~FlightFrontend() {
  FINFO << "Point A" << dendl;
  delete env.flight_server;
  FINFO << "Point B" << dendl;
  env.flight_server = nullptr;

  FINFO << "Point C" << dendl;
  delete env.flight_store;
  FINFO << "Point D" << dendl;
  env.flight_store = nullptr;
  FINFO << "Point E" << dendl;

  FINFO << "flight server shut down" << dendl;
}

int FlightFrontend::init() {
  if (port <= 0) {
    port = FlightServer::default_port;
  }
  const std::string url =
    std::string("grpc+tcp://localhost:") + std::to_string(port);
  auto r = flt::Location::Parse(url);
  if (!r.ok()) {
    FERROR << "could not parse server uri: " << url << dendl;
    return -EINVAL;
  }
  flt::Location location = *r;

#warning "This is where we set up the server's authentication."
  flt::FlightServerOptions options(location);
  options.verify_client = false;
  auto s = env.flight_server->Init(options);
  if (!s.ok()) {
    FERROR << "couldn't init flight server; status=" << s << dendl;
    return -EINVAL;
  }

  FINFO << "FlightServer inited; will use port " << port << dendl;
  return 0;
}

int FlightFrontend::run() {
  try {
    flight_thread = make_named_thread(server_thread_name,
				      &FlightServer::ServeAlt,
				      env.flight_server);

    FINFO << "FlightServer thread started, id=" <<
      flight_thread.get_id() <<
      ", joinable=" << flight_thread.joinable() << dendl;
    return 0;
  } catch (std::system_error& e) {
    FERROR << "FlightServer thread failed to start" << dendl;
    return -e.code().value();
  }
}

void FlightFrontend::stop() {
  arw::Status s;
  s = env.flight_server->Shutdown();
  if (!s.ok()) {
    FERROR << "call to Shutdown failed; status=" << s << dendl;
    return;
  }

  s = env.flight_server->Wait();
  if (!s.ok()) {
    FERROR << "call to Wait failed; status=" << s << dendl;
    return;
  }

  FINFO << "FlightServer shut down" << dendl;
}

void FlightFrontend::join() {
  flight_thread.join();
  FINFO << "FlightServer thread joined" << dendl;
}

void FlightFrontend::pause_for_new_config() {
  // ignore since config changes won't alter flight_server
}

void FlightFrontend::unpause_with_new_config() {
  // ignore since config changes won't alter flight_server
}

/* ************************************************************ */

FlightGetObj_Filter::FlightGetObj_Filter(const req_state* request,
					 RGWGetObj_Filter* next) :
  RGWGetObj_Filter(next),
  penv(request->penv),
  dp(request->cct->get(), dout_subsys, dout_prefix_str),
  current_offset(0),
  expected_size(request->obj_size),
  uri(request->decoded_uri),
  tenant_name(request->bucket->get_tenant()),
  bucket_name(request->bucket->get_name()),
  object_key(request->object->get_key()),
  user_id(request->user->get_id())
{
#warning "TODO: fix use of tmpnam"
  char name[L_tmpnam];
  const char* namep = std::tmpnam(name);

  if (namep) {
    temp_file_name = namep;
    temp_file.open(temp_file_name);
    FINFO << "temporary file with path " << temp_file_name <<
      " used for object " << object_key.name << dendl;
  } else {
    FERROR << "unable to generate temporary file name for \"" << name << "\"" << dendl;
  }
}

FlightGetObj_Filter::~FlightGetObj_Filter() {
  if (temp_file.is_open()) {
    temp_file.close();
  }

  // remove temporary file
  std::error_code error;
  std::filesystem::remove(temp_file_name, error);
  if (error) {
    FWARN << "FlightGetObj_Filter got error when removing temp file; "
      "error=" << error.value() <<
      ", temp_file_name=" << temp_file_name << dendl;
  }
}

int FlightGetObj_Filter::handle_data(bufferlist& bl,
				     off_t bl_ofs,
				     off_t bl_len)
{
  FINFO << "flight handling data from offset " <<
    current_offset << " (" << bl_ofs << ") of size " << bl_len << dendl;

  current_offset += bl_len;

  // this is a best-effort; if we fail to create a flight, we do not
  // introduce any erros into the chain of filters; we instead simply
  // log the issues
  if (temp_file.is_open()) {
    bl.write_stream(temp_file);

    if (current_offset >= expected_size) {
      FINFO << "data read is completed, current_offset=" <<
	current_offset << ", expected_size=" << expected_size << dendl;
      temp_file.close();

      // metadata to extract
      std::shared_ptr<const arw::KeyValueMetadata> kv_metadata;
      std::shared_ptr<arw::Schema> aw_schema;
      int64_t num_rows = 0;

      ObjectDescriptor obj_desc(bucket_name, object_key.get_oid(), tenant_name);

      // read the metadata in
      {
	auto result_open = arrow::io::ReadableFile::Open(temp_file_name);
	if (! result_open.ok()) {
	  FERROR << "unable to open file for reading; path: " << temp_file_name << dendl;
	  goto chain;
	}
	std::shared_ptr<arrow::io::ReadableFile> file = result_open.ValueOrDie();

      auto shim = std::make_shared<RandomAccessShim>(dp, file);
	auto metadata_result =
	  process_pq_metadata(shim, kv_metadata, aw_schema, num_rows, dp, obj_desc.to_string());

      auto close_status = shim->Close();
	if (! close_status.ok()) {
	  FWARN << "closing of shim layer resulted in error: " << close_status << dendl;
	}

	if (! metadata_result.ok()) {
	  FERROR << "metadata issue, error: " << metadata_result << dendl;
	  goto chain;
	}
      }

      auto flight_data_result = FlightData::create(obj_desc.get_flight_descriptor(),
						   aw_schema, kv_metadata, user_id,
						   num_rows, expected_size);
      if (! flight_data_result.ok()) {
	FERROR << "unable to create FlightData record for " << obj_desc << dendl;
	goto chain;
      }

      FlightStore* store = penv.flight_store;
      auto add_flight_result = store->add_flight(std::move(flight_data_result.ValueOrDie()));
      if (add_flight_result.ok()) {
	FINFO << "added flight with key " << add_flight_result.ValueOrDie() << dendl;
      } else {
	FERROR << "unable to add flight for " << obj_desc << dendl;
      }
    } // if last block
  } // if file opened

  // we must chain to the other GetObj_Filter's
chain:

  // chain to next filter in stream
  const int ret = RGWGetObj_Filter::handle_data(bl, bl_ofs, bl_len);

  return ret;
} // FlightGetObj_Filter::handle_data()

} // namespace rgw::flight
