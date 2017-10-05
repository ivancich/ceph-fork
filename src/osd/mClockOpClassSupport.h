// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#pragma once

#include <bitset>

#include "dmclock/src/dmclock_server.h"
#include "osd/OpRequest.h"

class PGQueueable;
class PGSnapTrim;
class PGScrub;
class PGRecovery;

namespace ceph {
  namespace mclock {

    enum class osd_op_type_t {
      client_op, osd_rep_op, bg_snaptrim, bg_recovery, bg_scrub
    };

    struct pg_queueable_visitor_t : public boost::static_visitor<osd_op_type_t> {
      osd_op_type_t operator()(const OpRequestRef& o) const;
      osd_op_type_t operator()(const PGSnapTrim& o) const;
      osd_op_type_t operator()(const PGScrub& o) const;
      osd_op_type_t operator()(const PGRecovery& o) const;
    }; // class pg_queueable_visitor_t

    class OpClassClientInfoMgr {
      crimson::dmclock::ClientInfo client_op;
      crimson::dmclock::ClientInfo osd_rep_op;
      crimson::dmclock::ClientInfo snaptrim;
      crimson::dmclock::ClientInfo recov;
      crimson::dmclock::ClientInfo scrub;

      static constexpr std::size_t rep_op_msg_bitset_size = 128;
      std::bitset<rep_op_msg_bitset_size> rep_op_msg_bitset;
      void add_rep_op_msg(int message_code);

      CephContext *cct;

    public:

      OpClassClientInfoMgr(CephContext *cct);

      inline crimson::dmclock::ClientInfo get_client_info(osd_op_type_t type) {
	switch(type) {
	case osd_op_type_t::client_op:
	  return client_op;
	case osd_op_type_t::osd_rep_op:
	  return osd_rep_op;
	case osd_op_type_t::bg_snaptrim:
	  return snaptrim;
	case osd_op_type_t::bg_recovery:
	  return recov;
	case osd_op_type_t::bg_scrub:
	  return scrub;
	default:
	  assert(0);
	  return crimson::dmclock::ClientInfo(-1, -1, -1);
	}
      }

      osd_op_type_t osd_op_type(const PGQueueable&) const;

      // used for debugging since faster implementation can be done
      // with rep_op_msg_bitmap
      static bool is_rep_op(uint16_t);
    }; // OpClassClientInfoMgr
  } // namespace mclock
} // namespace ceph
