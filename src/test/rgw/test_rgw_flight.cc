// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <errno.h>
#include <iostream>
#include <sstream>
#include <string>

#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "include/ceph_assert.h"
#include "gtest/gtest.h"

#include "rgw_flight.h"

#define dout_subsys ceph_subsys_rgw


bool verbose {false};

using namespace rgw::flight;


TEST(FLIGHT_DESCRIPTOR_PARSE, ALL_3_IN_1) {
  flt::FlightDescriptor fd1 = flt::FlightDescriptor::Path( { "tenant7:bucket3/dir9/dir11/obj1" } );
  arw::Result<ObjectDescriptor> r1 = ObjectDescriptor::from_flight_descriptor(fd1);
  ASSERT_TRUE(r1.ok());
  ObjectDescriptor od1 = r1.ValueOrDie();
  ASSERT_EQ(od1.tenant, "tenant7");
  ASSERT_EQ(od1.bucket, "bucket3");
  ASSERT_EQ(od1.object, "dir9/dir11/obj1");
}

TEST(FLIGHT_DESCRIPTOR_PARSE, JUST_2_IN_1) {
  flt::FlightDescriptor fd1 = flt::FlightDescriptor::Path( { "bucket3/dir9/dir11/obj1" } );
  arw::Result<ObjectDescriptor> r1 = ObjectDescriptor::from_flight_descriptor(fd1);
  ASSERT_TRUE(r1.ok());
  ObjectDescriptor od1 = r1.ValueOrDie();
  ASSERT_EQ(od1.tenant, "");
  ASSERT_EQ(od1.bucket, "bucket3");
  ASSERT_EQ(od1.object, "dir9/dir11/obj1");
}

TEST(FLIGHT_DESCRIPTOR_PARSE, JUST_2_IN_SEPARATE) {
  flt::FlightDescriptor fd1 = flt::FlightDescriptor::Path( { "bucket3", "dir9", "dir11", "obj1" } );
  arw::Result<ObjectDescriptor> r1 = ObjectDescriptor::from_flight_descriptor(fd1);
  ASSERT_TRUE(r1.ok());
  ObjectDescriptor od1 = r1.ValueOrDie();
  ASSERT_EQ(od1.tenant, "");
  ASSERT_EQ(od1.bucket, "bucket3");
  ASSERT_EQ(od1.object, "dir9/dir11/obj1");
}

TEST(FLIGHT_DESCRIPTOR_PARSE, 3_IN_SEPARATE) {
  flt::FlightDescriptor fd1 = flt::FlightDescriptor::Path( { "tenantwalter:bucket3", "dir9", "dir11", "obj1" } );
  arw::Result<ObjectDescriptor> r1 = ObjectDescriptor::from_flight_descriptor(fd1);
  ASSERT_TRUE(r1.ok());
  ObjectDescriptor od1 = r1.ValueOrDie();
  ASSERT_EQ(od1.tenant, "tenantwalter");
  ASSERT_EQ(od1.bucket, "bucket3");
  ASSERT_EQ(od1.object, "dir9/dir11/obj1");
}


TEST(FLIGHT_DESCRIPTOR_PARSE, COMBINED_AND_SEPARATE) {
  flt::FlightDescriptor fd1 = flt::FlightDescriptor::Path( { "tenantwalter:bucket3/dir9", "dir11", "obj1" } );
  arw::Result<ObjectDescriptor> r1 = ObjectDescriptor::from_flight_descriptor(fd1);
  ASSERT_TRUE(r1.ok());
  ObjectDescriptor od1 = r1.ValueOrDie();
  ASSERT_EQ(od1.tenant, "tenantwalter");
  ASSERT_EQ(od1.bucket, "bucket3");
  ASSERT_EQ(od1.object, "dir9/dir11/obj1");
}

TEST(FLIGHT_DESCRIPTOR_PARSE, COMBINED_AND_SEPARATE_2) {
  flt::FlightDescriptor fd1 = flt::FlightDescriptor::Path( { "bucket3/dir9", "dir11", "obj1" } );
  arw::Result<ObjectDescriptor> r1 = ObjectDescriptor::from_flight_descriptor(fd1);
  ASSERT_TRUE(r1.ok());
  ObjectDescriptor od1 = r1.ValueOrDie();
  ASSERT_EQ(od1.tenant, "");
  ASSERT_EQ(od1.bucket, "bucket3");
  ASSERT_EQ(od1.object, "dir9/dir11/obj1");
}

TEST(FLIGHT_DESCRIPTOR_PARSE, EMPTY) {
  flt::FlightDescriptor fd1 = flt::FlightDescriptor::Path( { } );
  arw::Result<ObjectDescriptor> r1 = ObjectDescriptor::from_flight_descriptor(fd1);
  ASSERT_FALSE(r1.ok());
}

TEST(FLIGHT_DESCRIPTOR_PARSE, JUST_BUCKET) {
  flt::FlightDescriptor fd1 = flt::FlightDescriptor::Path( { "bucket99" } );
  arw::Result<ObjectDescriptor> r1 = ObjectDescriptor::from_flight_descriptor(fd1);
  ASSERT_FALSE(r1.ok());
}

TEST(FLIGHT_DESCRIPTOR_PARSE, FLIPPED) {
  flt::FlightDescriptor fd1 = flt::FlightDescriptor::Path( { "bucket99/obj2:tenant77" } );
  arw::Result<ObjectDescriptor> r1 = ObjectDescriptor::from_flight_descriptor(fd1);
  ASSERT_FALSE(r1.ok());
}


int main(int argc, char *argv[])
{
  auto args = argv_to_vec(argc, argv);
  env_to_vec(args);

  std::string val;
  for (auto arg_iter = args.begin(); arg_iter != args.end();) {
    if (ceph_argparse_flag(args, arg_iter, "--verbose",
			      (char*) nullptr)) {
      verbose = true;
    } else {
      ++arg_iter;
    }
  }

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
