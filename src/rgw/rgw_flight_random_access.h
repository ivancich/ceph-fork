// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2024 IBM
 *
 * See file COPYING for licensing information.
 */

#pragma once


#include "common/dout.h"

#include "arrow/type.h"
#include "arrow/io/interfaces.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/future.h"

#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"

#include "rgw_flight_common.h"
#include "rgw_flight.h"


namespace rgw::flight {

/*
 * Allows us to insert a shim within a random access file, so we can
 * watch what it's doing and how it operates. It's for debugging and
 * learning and not production, although since it only produces log
 * output at level 20, it won't have a measureable impact.
 */
class RandomAccessShim : public arw::io::RandomAccessFile {
public:

  RandomAccessShim(
    const DoutPrefix _dp,
    const std::shared_ptr<arw::io::RandomAccessFile>& _source);

  // FileInterface interface
  virtual arw::Status Close() override;
  virtual arw::Result<int64_t> Tell() const override;
  virtual bool closed() const override;

  // Readable interface
  virtual arw::Result<int64_t> Read(int64_t nbytes, void* out) override;
  virtual arw::Result<std::shared_ptr<arw::Buffer>> Read(int64_t nbytes) override;

  // InputStream interface
  virtual bool supports_zero_copy() const override;

  // Seekable interface
  virtual arw::Status Seek(int64_t position) override;

  // RandomAccessFile interface
  virtual arw::Result<int64_t> GetSize() override;

private:

  const DoutPrefix dp;
  std::shared_ptr<arw::io::RandomAccessFile> source;

}; // class RandomAccessShim


/*
 * Provides the arrow RandomAccessFile interface to an RGW object via
 * the SAL interface; this is useful when parsing a parquet file to
 * learn the metadata, schema, etc.
 */
class RandomAccessObject : public arw::io::RandomAccessFile {

  // FlightData flight_data;
  const DoutPrefix& dp;

  int64_t position;
  bool is_closed;
  std::unique_ptr<rgw::sal::Object>& obj;
  std::unique_ptr<rgw::sal::Object::ReadOp> op;

public:

  RandomAccessObject(const DoutPrefix& _dp,
		     std::unique_ptr<rgw::sal::Object>& _obj) :
    // flight_data(_flight_data),
    dp(_dp),
    position(-1),
    is_closed(false),
    obj(_obj)
  {
    op = obj->get_read_op();
  }

  arw::Status Open();

  // implement InputStream

  arw::Status Close() override;
  arw::Result<int64_t> Tell() const override;
  arw::Result<int64_t> Read(int64_t nbytes, void* out) override;
  arw::Result<std::shared_ptr<arw::Buffer>> Read(int64_t nbytes) override;

  bool closed() const override {
    return is_closed;
  }

  bool supports_zero_copy() const override {
    return false;
  }

  // implement Seekable

  arw::Result<int64_t> GetSize() override;
  arw::Result<std::string_view> Peek(int64_t nbytes) override;

  // implement Seekable interface

  arw::Status Seek(int64_t new_position) override;
}; // class RandomAccessObject

} // namespace
