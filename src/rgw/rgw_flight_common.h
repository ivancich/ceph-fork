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

#include <iostream>

#include "arrow/type.h"
#include "arrow/result.h"
#include "arrow/flight/types.h"

namespace arw = arrow;
namespace flt = arrow::flight;

#define INFO_F(dp)   ldpp_dout(&dp, 20) << "INFO: " << __func__ << ": "
#define STATUS_F(dp) ldpp_dout(&dp, 10) << "STATUS: " << __func__ << ": "
#define WARN_F(dp)   ldpp_dout(&dp,  0) << "WARNING: " << __func__ << ": "
#define ERROR_F(dp)  ldpp_dout(&dp,  0) << "ERROR: " << __func__ << ": "

#define FINFO   INFO_F(dp)
#define FSTATUS STATUS_F(dp)
#define FWARN   WARN_F(dp)
#define FERROR  ERROR_F(dp)


namespace rgw::flight {

template<typename T>
std::ostream& operator<<(std::ostream& out, const arw::Result<T>& r) {
  if (r.ok()) {
    out << "value:" << r.ValueOrDie();
  } else {
    out << "status:" << r.status();
  }
  return out;
}

} // namespace rgw::flight
