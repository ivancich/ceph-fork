from flask import request
from flask_restful import Resource
from module import global_instance

class HelloWorld(Resource):
    _endpoint = '/'
    def get(self):
        return {'hello': 'world'}

class ClusterServer(Resource):
    _endpoint = '/cluster/server'
    def get(self):
        return global_instance().server_list()

class ClusterServerFqdn(Resource):
    _endpoint = '/cluster/server/<string:fqdn>'
    def retrieve(self, fqdn):
        return global_instance().get_server(fqdn)

class Todo(Resource):
    _endpoint = '/todo/<string:todo_id>'
    todos = {}
    def get(self, todo_id):
        if not todo_id:
            return self.todos
        elif todo_id in self.todos.keys():
            return {todo_id: self.todos[todo_id]}
        else:
            return {}

    def put(self, todo_id):
        self.todos[todo_id] = request.form['data']
        return {todo_id: self.todos[todo_id]}

