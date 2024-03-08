// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2024 IBM
 *
 * See file COPYING for licensing information.
 */

#include "rgw_flight_random_access.h"


namespace rgw::flight {


// A Buffer that owns its memory and frees it when the Buffer is
// destructed
class OwnedBuffer : public arw::Buffer {

  uint8_t* buffer;

protected:

  OwnedBuffer(uint8_t* _buffer, int64_t _size) :
    Buffer(_buffer, _size),
    buffer(_buffer)
    { }

public:

  ~OwnedBuffer() override {
    delete[] buffer;
  }

  static arw::Result<std::shared_ptr<OwnedBuffer>> make(int64_t size) {
    uint8_t* buffer = new (std::nothrow) uint8_t[size];
    if (!buffer) {
      return arw::Status::OutOfMemory("could not allocated buffer of size %" PRId64, size);
    }

    OwnedBuffer* ptr = new OwnedBuffer(buffer, size);
    std::shared_ptr<OwnedBuffer> result;
    result.reset(ptr);
    return result;
  }

  // if what's read in is less than capacity
  void set_size(int64_t size) {
    size_ = size;
  }

  // pointer that can be used to write into buffer
  uint8_t* writeable_data() {
    return buffer;
  }
}; // class OwnedBuffer


RandomAccessShim::RandomAccessShim(const DoutPrefix _dp,
				   const std::shared_ptr<arw::io::RandomAccessFile>& _source) :
  dp(_dp),
  source(_source)
{ }

arw::Status RandomAccessShim::Close() {
  auto result = source->Close();
  FINFO << "shim asked to close and got " << result << dendl;
  return result;
}

arw::Result<int64_t> RandomAccessShim::Tell() const {
  auto result = source->Tell();
  FINFO << "shim tell returns " << result << dendl;
  return result;
}

bool RandomAccessShim::closed() const {
  auto result = source->closed();
  FINFO << "shim closed returns " << result << dendl;
  return result;
}

// Readable interface
arw::Result<int64_t> RandomAccessShim::Read(int64_t nbytes, void* out) {
  auto result = source->Read(nbytes, out);
  FINFO << "shim requested " << nbytes << " bytes and got " << result << dendl;
  return result;
}

arw::Result<std::shared_ptr<arw::Buffer>> RandomAccessShim::Read(int64_t nbytes) {
  auto result = source->Read(nbytes);
  FINFO << "shim read by requesting " << nbytes << " bytes" << dendl;
  return result;
}

// InputStream interface
bool RandomAccessShim::supports_zero_copy() const {
  auto result = source->supports_zero_copy();
  FINFO << "shim asked if zero-copy is supported and got " << result << dendl;
  return result;
}

// Seekable interface
arw::Status RandomAccessShim::Seek(int64_t position) {
  auto status = source->Seek(position);
  FINFO << "shim asked to seek to " << position << " and got " << status << dendl;
  return status;
}

arw::Result<int64_t> RandomAccessShim::GetSize() {
  auto result = source->GetSize();
  FINFO << "shim did getsize and got " << result << dendl;
  return result;
}


arw::Status RandomAccessObject::Open() {
  int ret = op->prepare(null_yield, &dp);
  if (ret < 0) {
    return arw::Status::IOError(
      "unable to prepare object with error %d", ret);
  }
  FINFO << "file opened successfully" << dendl;
  position = 0;
  return arw::Status::OK();
}

arw::Status RandomAccessObject::Close() {
  position = -1;
  is_closed = true;
  (void) op.reset();
  FINFO << "object closed" << dendl;
  return arw::Status::OK();
}

arw::Result<int64_t> RandomAccessObject::Tell() const {
  if (position < 0) {
    return arw::Status::IOError("could not determine position");
  } else {
    return position;
  }
}

arw::Result<int64_t> RandomAccessObject::Read(int64_t nbytes, void* out) {
  FINFO << "entered: asking for " << nbytes << " bytes" << dendl;

  if (position < 0) {
    FERROR << "error, position indicated error" << dendl;
    return arw::Status::IOError("object read op is in bad state");
  }

  // note: read function reads through end_position inclusive
  int64_t end_position = position + nbytes - 1;

  bufferlist bl;

  const int64_t bytes_read =
    op->read(position, end_position, bl, null_yield, &dp);
  if (bytes_read < 0) {
    const int64_t former_position = position;
    position = -1;
    FERROR << "read operation returned " << bytes_read << dendl;
    return arw::Status::IOError(
      "unable to read object at position %" PRId64 ", error code: %" PRId64,
      former_position,
      bytes_read);
  }

  // TODO: see if there's a way to get rid of this copy, perhaps
  // updating rgw::sal::read_op
  bl.cbegin().copy(bytes_read, reinterpret_cast<char*>(out));

  position += bytes_read;

  if (nbytes != bytes_read) {
    FINFO << "partial read: nbytes=" << nbytes <<
      ", bytes_read=" << bytes_read << dendl;
  }
  FINFO << bytes_read << " bytes read" << dendl;
  return bytes_read;
}

arw::Result<std::shared_ptr<arw::Buffer>> RandomAccessObject::Read(int64_t nbytes) {
  FINFO << "entered: asking for " << nbytes << " bytes" << dendl;

  std::shared_ptr<OwnedBuffer> buffer;
  ARROW_ASSIGN_OR_RAISE(buffer, OwnedBuffer::make(nbytes));

  ARROW_ASSIGN_OR_RAISE(const int64_t bytes_read,
			Read(nbytes, buffer->writeable_data()));
  buffer->set_size(bytes_read);

  return buffer;
}

arw::Result<int64_t> RandomAccessObject::GetSize() {
  auto result = obj->get_size();
  FINFO << "entered: returning " << result << dendl;
  return result;
}

arw::Result<std::string_view> RandomAccessObject::Peek(int64_t nbytes) {
  FINFO << "entered: " << nbytes << " bytes" << dendl;

  int64_t saved_position = position;

  ARROW_ASSIGN_OR_RAISE(OwningStringView buffer,
			OwningStringView::make(nbytes));

  ARROW_ASSIGN_OR_RAISE(const int64_t bytes_read,
			Read(nbytes, (void*) buffer.writeable_data()));

  // restore position for a peek
  position = saved_position;

  if (bytes_read < nbytes) {
    // create new OwningStringView with moved buffer
    return OwningStringView::shrink(std::move(buffer), bytes_read);
  } else {
    return buffer;
  }
}

arw::Status RandomAccessObject::Seek(int64_t new_position) {
  FINFO << "entered: position: " << new_position << dendl;
  if (position < 0) {
    FERROR << "position indicated bad state for RandomAccessObject" << dendl;
    return arw::Status::IOError("object read op is in bad state");
  } else {
    position = new_position;
    return arw::Status::OK();
  }
}

} // namespace
