"""
A RESTful API for Ceph
"""

# We must share a global reference to this instance, because it is the
# gatekeeper to all accesses to data from the C++ side (e.g. the REST API
# request handlers need to see it)
_global_instance = {'plugin': None}
def global_instance():
    assert _global_instance['plugin'] is not None
    return _global_instance['plugin']

import json
import uuid
import errno

import api

from flask import Flask, request
from flask_restful import Api

from logger import logger

from mgr_module import MgrModule


class Module(MgrModule):

    COMMANDS = [
            {
                "cmd": "enable_auth "
                       "name=val,type=CephChoices,strings=true|false",
                "desc": "Set whether to authenticate API access by key",
                "perm": "rw"
            },
            {
                "cmd": "auth_key_create "
                       "name=key_name,type=CephString",
                "desc": "Create an API key with this name",
                "perm": "rw"
            },
            {
                "cmd": "auth_key_delete "
                       "name=key_name,type=CephString",
                "desc": "Delete an API key with this name",
                "perm": "rw"
            },
            {
                "cmd": "auth_key_list",
                "desc": "List all API keys",
                "perm": "rw"
            },
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        _global_instance['plugin'] = self
        self.log.info("Constructing module {0}: instance {1}".format(
            __name__, _global_instance))
        #self.requests = RequestCollection()

        self.keys = {}
        self.enable_auth = True
        self.app = None
        self.api = None

    def get_authenticators(self):
        """
        For the benefit of django rest_framework APIView classes
        """
        return [self._auth_cls()]

    def shutdown(self):
        # We can shutdown the underlying werkzeug server
        _shutdown = request.environ.get('werkzeug.server.shutdown')
        if _shutdown is None:
            raise RuntimeError('Not running with the Werkzeug Server')
        _shutdown()

    def serve(self):
        self.keys = self._load_keys()
        self.enable_auth = self.get_config_json("enable_auth")
        if self.enable_auth is None:
            self.enable_auth = True

        self.app = Flask('ceph-mgr')
        self.api = Api(self.app)

        # Add the resources as defined in api module
        for obj in dir(api):
            _endpoint = getattr(obj, '_endpoint')
            if _endpoint:
                api.add_resource(obj, _endpoint)

        from rest_framework import authentication

        class KeyUser(object):
            def __init__(self, username):
                self.username = username

            def is_authenticated(self):
                return True

        # Take a local reference to use inside the APIKeyAuthentication
        # class definition
        log = self.log

        class APIKeyAuthentication(authentication.BaseAuthentication):
            def authenticate(self, request):
                if not global_instance().enable_auth:
                    return KeyUser("anonymous"), None

                username = request.META.get('HTTP_X_USERNAME')
                if not username:
                    log.warning("Rejecting: no X_USERNAME")
                    return None

                if username not in global_instance().keys:
                    log.warning("Rejecting: username does not exist")
                    return None

                api_key = request.META.get('HTTP_X_APIKEY')
                expect_key = global_instance().keys[username]
                if api_key != expect_key:
                    log.warning("Rejecting: wrong API key")
                    return None

                log.debug("Accepted for user {0}".format(username))
                return KeyUser(username), None

        self._auth_cls = APIKeyAuthentication

        self.app.run(host='0.0.0.0', port=8002, debug=True)

    def _generate_key(self):
        return uuid.uuid4().__str__()

    def _load_keys(self):
        loaded_keys = self.get_config_json("keys")
        self.log.debug("loaded_keys: {0}".format(loaded_keys))
        if loaded_keys is None:
            return {}
        else:
            return loaded_keys

    def _save_keys(self):
        self.set_config_json("keys", self.keys)

    def handle_command(self, cmd):
        self.log.info("handle_command: {0}".format(json.dumps(cmd, indent=2)))
        prefix = cmd['prefix']
        if prefix == "enable_auth":
            enable = cmd['val'] == "true"
            self.set_config_json("enable_auth", enable)
            self.enable_auth = enable
            return 0, "", ""
        elif prefix == "auth_key_create":
            if cmd['key_name'] in self.keys:
                return 0, self.keys[cmd['key_name']], ""
            else:
                self.keys[cmd['key_name']] = self._generate_key()
                self._save_keys()

            return 0, self.keys[cmd['key_name']], ""
        elif prefix == "auth_key_delete":
            if cmd['key_name'] in self.keys:
                del self.keys[cmd['key_name']]
                self._save_keys()

            return 0, "", ""
        elif prefix == "auth_key_list":
            return 0, json.dumps(self._load_keys(), indent=2), ""
        else:
            return -errno.EINVAL, "", "Command not found '{0}'".format(prefix)
