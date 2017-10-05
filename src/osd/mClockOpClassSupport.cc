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


#include "common/dout.h"
#include "osd/mClockOpClassSupport.h"
#include "osd/PGQueueable.h"

#include "include/assert.h"

namespace ceph {

  namespace mclock {

    pg_queueable_visitor_t pg_queueable_visitor;

    OpClassClientInfoMgr::OpClassClientInfoMgr(CephContext *cct) :
      client_op(cct->_conf->osd_op_queue_mclock_client_op_res,
		cct->_conf->osd_op_queue_mclock_client_op_wgt,
		cct->_conf->osd_op_queue_mclock_client_op_lim),
      osd_rep_op(cct->_conf->osd_op_queue_mclock_osd_rep_op_res,
		 cct->_conf->osd_op_queue_mclock_osd_rep_op_wgt,
		 cct->_conf->osd_op_queue_mclock_osd_rep_op_lim),
      snaptrim(cct->_conf->osd_op_queue_mclock_snap_res,
	       cct->_conf->osd_op_queue_mclock_snap_wgt,
	       cct->_conf->osd_op_queue_mclock_snap_lim),
      recov(cct->_conf->osd_op_queue_mclock_recov_res,
	    cct->_conf->osd_op_queue_mclock_recov_wgt,
	    cct->_conf->osd_op_queue_mclock_recov_lim),
      scrub(cct->_conf->osd_op_queue_mclock_scrub_res,
	    cct->_conf->osd_op_queue_mclock_scrub_wgt,
	    cct->_conf->osd_op_queue_mclock_scrub_lim),
      cct(cct)
    {
      static constexpr int rep_ops[] = {
	MSG_OSD_REPOP,
	MSG_OSD_REPOPREPLY,
	MSG_OSD_PG_UPDATE_LOG_MISSING,
	MSG_OSD_PG_UPDATE_LOG_MISSING_REPLY,
	MSG_OSD_EC_WRITE,
	MSG_OSD_EC_WRITE_REPLY,
	MSG_OSD_EC_READ,
	MSG_OSD_EC_READ_REPLY
      };
      for (auto op : rep_ops) {
	add_rep_op_msg(op);
      }

      lgeneric_subdout(cct, osd, 20) <<
	"mClock OpClass settings:: " <<
	"client_op:" << client_op <<
	"; osd_rep_op:" << osd_rep_op <<
	"; snaptrim:" << snaptrim <<
	"; recov:" << recov <<
	"; scrub:" << scrub <<
	dendl;

      lgeneric_subdout(cct, osd, 30) <<
	"mClock OpClass message bit set:: " <<
	rep_op_msg_bitset.to_string() << dendl;
    }

    void OpClassClientInfoMgr::add_rep_op_msg(int message_code) {
      assert(message_code >= 0 && message_code < int(rep_op_msg_bitset_size));
      rep_op_msg_bitset.set(message_code);
    }

    osd_op_type_t
    OpClassClientInfoMgr::osd_op_type(const PGQueueable& pgq) const {
      osd_op_type_t type =
	boost::apply_visitor(pg_queueable_visitor, pgq.get_variant());
      if (osd_op_type_t::client_op != type) {
	return type;
      } else {
	// get_header returns ceph_msg_header type, ceph_msg_header
	// stores type as unsigned little endian, so be sure to
	// convert to CPU byte ordering
	OpRequestRef op_ref = boost::get<OpRequestRef>(pgq.get_variant());
	__le16 mtype_le = op_ref->get_req()->get_header().type;
	__u16 mtype = le16_to_cpu(mtype_le);
	if (rep_op_msg_bitset.test(mtype)) {
#if 1 // TEMPORARY
	  assert(is_rep_op(mtype));
	  lgeneric_subdout(cct, osd, 1) << "OSD_REP_OP: " <<
	    *op_ref->get_req() << dendl;
#endif
	  return osd_op_type_t::osd_rep_op;
	} else {
#if 1 // TEMPORARY
	  assert(! is_rep_op(mtype));
#endif
	  return osd_op_type_t::client_op;
	}
      }
    }

    // used for debugging since faster implementation can be done
    // with rep_op_msg_bitmap
    bool OpClassClientInfoMgr::is_rep_op(uint16_t mtype) {
      return
	MSG_OSD_REPOP == mtype ||
	MSG_OSD_REPOPREPLY == mtype ||
	MSG_OSD_PG_UPDATE_LOG_MISSING == mtype ||
	MSG_OSD_PG_UPDATE_LOG_MISSING_REPLY == mtype ||
	MSG_OSD_EC_WRITE == mtype ||
	MSG_OSD_EC_WRITE_REPLY == mtype ||
	MSG_OSD_EC_READ == mtype ||
	MSG_OSD_EC_READ_REPLY == mtype;
    }

    osd_op_type_t
    pg_queueable_visitor_t::operator()(const OpRequestRef& o) const {
      // don't know if it's a client_op or an osd_rep_op
      return osd_op_type_t::client_op;
    }

    osd_op_type_t
    pg_queueable_visitor_t::operator()(const PGSnapTrim& o) const {
      return osd_op_type_t::bg_snaptrim;
    }

    osd_op_type_t
    pg_queueable_visitor_t::operator()(const PGScrub& o) const {
      return osd_op_type_t::bg_scrub;
    }

    osd_op_type_t
    pg_queueable_visitor_t::operator()(const PGRecovery& o) const {
      return osd_op_type_t::bg_recovery;
    }
  } // namespace mclock
} // namespace ceph
